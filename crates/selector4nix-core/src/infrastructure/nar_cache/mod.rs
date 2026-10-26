//! NAR disk cache facade (spec §1, §3).
//!
//! Sits between the HTTP layer and the regular NAR streaming path. For every client NAR
//! request it picks one of four routes:
//!
//! 1. *bypass* — the feature is disabled/unusable, or no narinfo-derived expectation
//!    (`FileHash`/`FileSize`) is known: plain real-time upstream streaming, byte-for-byte
//!    identical to the behavior without this feature;
//! 2. *miss* — no usable partial on disk: real-time upstream streaming, with the stream
//!    teed to disk by the single append-license holder;
//! 3. *partial-hit* — a `[0, W)` prefix is on disk: a filler resumes the upstream download
//!    from `W`, verifies the completed file against the narinfo `FileHash`, and serves
//!    waiters from disk;
//! 4. *complete* — the whole file is already on disk: verify, serve from disk, unlink.
//!
//! Invariants (spec §0): partial bytes are never sent to a client unverified; verified
//! files are one-shot (served then removed); nothing is downloaded without a waiting
//! consumer; already-persisted `[0, W)` bytes are never fetched from upstream again except
//! for the single refetch-after-verification-failure of spec §5.

mod storage;
mod supervisor;
mod sweep;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::Result as AnyhowResult;
use bytes::Bytes;
use futures::Stream;
use tokio::task::JoinHandle;

use crate::AppError;
use crate::domain::common::passthrough_headers::PassthroughHeaders;
use crate::domain::common::query_parameters::QueryParameters;
use crate::domain::common::url::Url;
use crate::domain::nar_file::NarFileRepository;
use crate::domain::nar_file::model::NarFileKey;
use crate::domain::nar_file::port::{NarStreamData, NarStreamHeaders, NarStreamProvider};
use crate::domain::nar_info::NarInfoRepository;
use crate::domain::nar_info::model::StorePathHash;
use crate::domain::substituter::SubstituterRepository;
use crate::infrastructure::config::NarDiskCacheConfiguration;
use crate::infrastructure::metric::NarCacheMetric;
use crate::infrastructure::nar_cache::storage::{
    NarCacheStorage, PartialAppender, PartialState, VerifyOutcome,
};
use crate::infrastructure::nar_cache::supervisor::{
    AppendSupervisor, FillDeps, FillPlan, FillResult, TeeGuard,
};

/// Upper bound on inspect→fill→re-inspect rounds per request; each `Ended` outcome consumes
/// one round. Legitimate flows need at most two.
const MAX_SERVE_ROUNDS: usize = 8;

const SWEEP_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Repeatable opener for the regular real-time upstream stream (the unchanged non-cache
/// path). Boxed because a request may fall back to it after cache rounds.
pub type OpenUpstream = Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = Result<NarStreamData, AppError>> + Send>> + Send + Sync,
>;

/// File identity expected from the narinfo of the NAR being requested.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NarExpectation {
    /// sha256 of the whole NAR file, hex.
    pub file_hash: String,
    pub file_size: u64,
}

impl NarExpectation {
    /// Extract `FileHash`/`FileSize` from raw narinfo content. Returns `None` when either
    /// field is absent, the hash algorithm is not sha256, or the values are malformed —
    /// such NARs bypass the cache (spec §3).
    pub fn from_nar_info_content(content: &str) -> Option<Self> {
        let mut file_hash = None;
        let mut file_size = None;
        for line in content.lines() {
            if let Some(value) = line.strip_prefix("FileHash:") {
                let Some((algorithm, hash)) = value.trim().split_once(':') else {
                    continue;
                };
                if !algorithm.eq_ignore_ascii_case("sha256") {
                    return None;
                }
                if hash.len() != 64 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
                    return None;
                }
                file_hash = Some(hash.to_ascii_lowercase());
            } else if let Some(value) = line.strip_prefix("FileSize:") {
                file_size = value.trim().parse::<u64>().ok();
            }
        }
        Some(Self {
            file_hash: file_hash?,
            file_size: file_size?,
        })
    }
}

/// A stream handed to the HTTP layer plus the store path hash behind it (best-effort, used
/// for transfer metrics only).
pub struct ServedNar {
    pub data: NarStreamData,
    pub store_path_hash: Option<StorePathHash>,
}

/// The facade used by the NAR streaming use case.
pub struct NarDiskCache {
    inner: Option<NarDiskCacheInner>,
}

struct NarDiskCacheInner {
    config: NarDiskCacheConfiguration,
    storage: Arc<NarCacheStorage>,
    supervisor: Arc<AppendSupervisor>,
    deps: Arc<FillDeps>,
    nar_file_repository: Arc<dyn NarFileRepository>,
    nar_info_repository: Arc<dyn NarInfoRepository>,
    metric: Arc<NarCacheMetric>,
}

impl NarDiskCache {
    /// A cache that passes every request straight through (feature disabled).
    pub fn disabled() -> Self {
        Self { inner: None }
    }

    /// Build an enabled cache. When the storage directory cannot be created or written, the
    /// feature is bypassed with a single `WARN` and the server keeps serving normally
    /// (spec §1).
    pub fn new(
        config: NarDiskCacheConfiguration,
        provider: Arc<dyn NarStreamProvider>,
        substituter_repository: Arc<dyn SubstituterRepository>,
        nar_file_repository: Arc<dyn NarFileRepository>,
        nar_info_repository: Arc<dyn NarInfoRepository>,
        metric: Arc<NarCacheMetric>,
    ) -> Self {
        if !config.enabled {
            return Self::disabled();
        }
        let storage = match NarCacheStorage::open(&config.dir, config.fsync_interval_bytes) {
            Ok(storage) => Arc::new(storage),
            Err(err) => {
                tracing::warn!(dir = %config.dir.display(), %err, "nar cache bypass: cache directory is not writable");
                return Self::disabled();
            }
        };
        let supervisor = Arc::new(AppendSupervisor::new());
        let deps = Arc::new(FillDeps {
            provider,
            substituter_repository,
            storage: Arc::clone(&storage),
            supervisor: Arc::clone(&supervisor),
        });
        tracing::info!(dir = %config.dir.display(), max_size_bytes = config.max_size_bytes, "nar disk cache enabled");
        Self {
            inner: Some(NarDiskCacheInner {
                config,
                storage,
                supervisor,
                deps,
                nar_file_repository,
                nar_info_repository,
                metric,
            }),
        }
    }

    /// Whether the cache participates in request handling at all. When false the caller
    /// keeps its entirely unmodified streaming path (spec §3, "disabled" branch).
    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// Periodic eviction task (spec §6): expired partials first, then LRU by last progress
    /// until the size budget holds. A no-op handle when disabled.
    pub fn spawn_sweep_task(self: &Arc<Self>) -> JoinHandle<()> {
        let Some(inner) = self.inner.as_ref() else {
            return tokio::spawn(async {});
        };
        let storage = Arc::clone(&inner.storage);
        let supervisor = Arc::clone(&inner.supervisor);
        let retention_secs = inner.config.partial_retention_secs;
        let max_size_bytes = inner.config.max_size_bytes;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(SWEEP_INTERVAL);
            loop {
                interval.tick().await;
                let active = supervisor.active_names();
                let report = storage.sweep(&active, retention_secs, max_size_bytes);
                if report.deleted > 0 {
                    tracing::info!(
                        deleted = report.deleted,
                        reclaimed_bytes = report.reclaimed_bytes,
                        "nar cache sweep"
                    );
                }
            }
        })
    }

    /// Route one client NAR request through the state machine of spec §3.
    pub async fn serve(
        &self,
        key: NarFileKey,
        upstream_query: Option<QueryParameters>,
        headers: PassthroughHeaders,
        open_upstream: OpenUpstream,
    ) -> Result<ServedNar, AppError> {
        let Some(inner) = self.inner.as_ref() else {
            let data = open_upstream().await?;
            return Ok(ServedNar {
                data,
                store_path_hash: None,
            });
        };

        let name = key.to_file_name();
        let name_value = name.value().to_string();

        let nar_file = inner.nar_file_repository.get(&key).await.ok().flatten();
        let store_path_hash = nar_file
            .as_ref()
            .and_then(|file| file.store_path_hash().cloned());
        let expectation = match store_path_hash.as_ref() {
            Some(hash) => inner.lookup_expectation(hash).await,
            None => None,
        };

        let Some(expectation) = expectation else {
            // No narinfo-derived expectation (rare: the narinfo is not cached or lacks
            // FileHash/FileSize) — pure real-time streaming, no disk writes (spec §3).
            tracing::debug!(nar_file = %name_value, "nar cache bypass: no narinfo file hash/size known");
            let data = open_upstream().await?;
            return Ok(ServedNar {
                data,
                store_path_hash,
            });
        };

        for _ in 0..MAX_SERVE_ROUNDS {
            match inner.storage.inspect(&name_value, expectation.file_size) {
                PartialState::Absent | PartialState::Partial { watermark: 0 } => {
                    let data = inner
                        .serve_miss(&name_value, &expectation, &open_upstream)
                        .await?;
                    return Ok(ServedNar {
                        data,
                        store_path_hash,
                    });
                }
                PartialState::Partial { watermark } => {
                    tracing::info!(nar_file = %name_value, from = watermark, total = expectation.file_size, "nar cache resumed");
                    inner.metric.record_partial_hit(watermark);
                    let plan = FillPlan {
                        nar_file_name: name.clone(),
                        file_hash: expectation.file_hash.clone(),
                        file_size: expectation.file_size,
                        start_offset: watermark,
                        upstream_query: upstream_query.clone(),
                        headers: headers.clone(),
                    };
                    match inner
                        .supervisor
                        .join_fill(&plan, Arc::clone(&inner.deps))
                        .await
                    {
                        FillResult::Ready { file, total } => {
                            let serve_file = file.try_clone().map_err(|err| {
                                AppError::chain_infrastructure(
                                    err,
                                    "failed to open serving handle for cached nar",
                                )
                            })?;
                            return Ok(ServedNar {
                                data: disk_serve_data(&name_value, serve_file, total),
                                store_path_hash,
                            });
                        }
                        // The append episode ended (a teeing client disconnected, or a
                        // cancelled filler); re-inspect and take over from the new state.
                        FillResult::Ended => continue,
                        FillResult::FallbackNeeded => {
                            let data = open_upstream().await?;
                            return Ok(ServedNar {
                                data,
                                store_path_hash,
                            });
                        }
                        FillResult::NotFound => {
                            return Err(AppError::not_found(
                                "nar file not found in any substituter while filling cache",
                            ));
                        }
                        FillResult::Failed(message) => {
                            return Err(AppError::infrastructure(format!(
                                "nar cache fill failed: {message}"
                            )));
                        }
                    }
                }
                PartialState::Complete { .. } => {
                    match inner
                        .storage
                        .verify(&name_value, &expectation.file_hash, expectation.file_size)
                        .await
                    {
                        Ok(VerifyOutcome::Match(file)) => {
                            inner.storage.delete(&name_value);
                            inner.metric.record_disk_hit();
                            tracing::info!(nar_file = %name_value, size = expectation.file_size, "nar cache verified & served");
                            return Ok(ServedNar {
                                data: disk_serve_data(&name_value, file, expectation.file_size),
                                store_path_hash,
                            });
                        }
                        Ok(VerifyOutcome::Mismatch { actual }) => {
                            tracing::warn!(
                                nar_file = %name_value,
                                expected = %expectation.file_hash,
                                actual,
                                "nar cache verify failed, evicting & refetching"
                            );
                            inner.storage.delete(&name_value);
                            // Self-heal: one full refetch preferring untried substituters
                            // (spec §5), modeled as a zero-offset fill.
                            let plan = FillPlan {
                                nar_file_name: name.clone(),
                                file_hash: expectation.file_hash.clone(),
                                file_size: expectation.file_size,
                                start_offset: 0,
                                upstream_query: upstream_query.clone(),
                                headers: headers.clone(),
                            };
                            match inner
                                .supervisor
                                .join_fill(&plan, Arc::clone(&inner.deps))
                                .await
                            {
                                FillResult::Ready { file, total } => {
                                    inner.metric.record_disk_hit();
                                    let serve_file = file.try_clone().map_err(|err| {
                                        AppError::chain_infrastructure(
                                            err,
                                            "failed to open serving handle for cached nar",
                                        )
                                    })?;
                                    return Ok(ServedNar {
                                        data: disk_serve_data(&name_value, serve_file, total),
                                        store_path_hash,
                                    });
                                }
                                FillResult::Ended => continue,
                                FillResult::FallbackNeeded => {
                                    let data = open_upstream().await?;
                                    return Ok(ServedNar {
                                        data,
                                        store_path_hash,
                                    });
                                }
                                FillResult::NotFound => {
                                    return Err(AppError::not_found(
                                        "nar file disappeared from substituters while refilling cache",
                                    ));
                                }
                                FillResult::Failed(message) => {
                                    return Err(AppError::infrastructure(format!(
                                        "nar cache refetch failed: {message}"
                                    )));
                                }
                            }
                        }
                        Err(err) => {
                            return Err(AppError::chain_infrastructure(
                                err,
                                "failed to verify cached nar partial",
                            ));
                        }
                    }
                }
            }
        }
        Err(AppError::infrastructure(
            "nar cache request handling did not converge",
        ))
    }
}

impl NarDiskCacheInner {
    async fn lookup_expectation(&self, hash: &StorePathHash) -> Option<NarExpectation> {
        let nar_info = self.nar_info_repository.get(hash).await.ok()??;
        let content = nar_info.nar_info()?.content();
        NarExpectation::from_nar_info_content(content)
    }

    /// Miss route: stream real-time upstream, teed to disk when this request holds the
    /// append license (spec §3 miss branch, spec §4 concurrent-miss rule).
    async fn serve_miss(
        &self,
        name: &str,
        expectation: &NarExpectation,
        open_upstream: &OpenUpstream,
    ) -> Result<NarStreamData, AppError> {
        let data = open_upstream().await?;

        // The append license is the single-writer guarantee; a concurrent miss streams
        // real-time without teeing (spec §4).
        if let Some(guard) = self.supervisor.acquire_tee(name) {
            match self.storage.create_append(name, expectation.file_size) {
                Ok(appender) => {
                    let stream = NarTeeStream::new(data.inner, appender, guard, name.to_string());
                    return Ok(NarStreamData::new(
                        data.headers,
                        Box::pin(stream),
                        data.source_url,
                    ));
                }
                Err(err) => {
                    tracing::warn!(nar_file = %name, %err, "nar cache bypass: could not open partial file for appending");
                    // `guard` drops here, releasing the slot.
                }
            }
        } else {
            tracing::debug!(nar_file = %name, "nar cache concurrent miss: streaming without disk append");
        }
        Ok(data)
    }
}

fn disk_serve_data(name: &str, file: std::fs::File, total: u64) -> NarStreamData {
    NarStreamData::new(
        NarStreamHeaders {
            content_length: Some(total),
            content_type: None,
            content_encoding: None,
        },
        Box::pin(DiskNarStream::new(file, total)),
        synthetic_cache_url(name),
    )
}

fn synthetic_cache_url(name: &str) -> Url {
    Url::new(&format!(
        "http://nar-disk-cache.selector4nix.internal/{name}"
    ))
    .expect("synthetic cache URL should be valid")
}

/// Wraps the real-time upstream stream and appends every forwarded chunk to the partial
/// file. Disk problems disable the tee silently mid-stream — the client stream is never
/// affected by cache failures (spec §1).
struct NarTeeStream {
    inner: Pin<Box<dyn Stream<Item = AnyhowResult<Bytes>> + Send>>,
    tee: Option<NarTee>,
}

struct NarTee {
    /// Held for its `Drop` (ends the episode and releases the single-flight slot);
    /// never read directly.
    #[expect(dead_code)]
    guard: TeeGuard,
    appender: PartialAppender,
    name: String,
}

impl NarTeeStream {
    fn new(
        inner: Pin<Box<dyn Stream<Item = AnyhowResult<Bytes>> + Send>>,
        appender: PartialAppender,
        guard: TeeGuard,
        name: String,
    ) -> Self {
        Self {
            inner,
            tee: Some(NarTee {
                guard,
                appender,
                name,
            }),
        }
    }

    fn end_tee(&mut self, reached_eof: bool) {
        if let Some(mut tee) = self.tee.take() {
            let watermark = tee.appender.watermark();
            let completed = reached_eof && tee.appender.expected_total() == watermark;
            tee.appender.finish(reached_eof);
            if completed {
                tracing::info!(nar_file = %tee.name, watermark, "nar cache partial completed via tee");
            } else {
                tracing::info!(nar_file = %tee.name, watermark, "nar cache partial retained");
            }
        }
    }
}

impl Stream for NarTeeStream {
    type Item = AnyhowResult<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                let append_failed = this
                    .tee
                    .as_mut()
                    .is_some_and(|tee| tee.appender.append(&bytes).is_err());
                if append_failed && let Some(tee) = this.tee.take() {
                    tracing::warn!(nar_file = %tee.name, "nar cache tee: append failed, continuing without caching");
                }
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(Err(err))) => {
                this.end_tee(false);
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(None) => {
                this.end_tee(true);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for NarTeeStream {
    fn drop(&mut self) {
        // Client disconnected mid-stream (spec §3): retain the partial at the current
        // watermark; the dropped `TeeGuard` wakes any waiters.
        self.end_tee(false);
    }
}

const DISK_READ_CHUNK_BYTES: usize = 256 * 1024;

/// An in-flight blocking read from [`DiskNarStream`]; hands the fd back together with
/// the bytes so the stream stays `'static` across `poll_next` calls.
type DiskReadTask = JoinHandle<std::io::Result<(std::fs::File, Vec<u8>)>>;

/// Streams `total` bytes of an open (possibly already unlinked) file through blocking
/// reads on the tokio blocking pool. The fd is private to this stream, so independent
/// waiters read their own copy independently (spec §4).
struct DiskNarStream {
    file: Option<std::fs::File>,
    remaining: u64,
    pending: Option<DiskReadTask>,
}

impl DiskNarStream {
    fn new(file: std::fs::File, total: u64) -> Self {
        Self {
            file: Some(file),
            remaining: total,
            pending: None,
        }
    }
}

impl Stream for DiskNarStream {
    type Item = AnyhowResult<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.remaining == 0 {
            return Poll::Ready(None);
        }

        if this.pending.is_none() {
            let file = this
                .file
                .take()
                .expect("`file` must be present while no read is pending");
            let len = (this.remaining as usize).min(DISK_READ_CHUNK_BYTES);
            this.pending = Some(tokio::task::spawn_blocking(move || {
                let mut file = file;
                let mut buffer = vec![0u8; len];
                let read = std::io::Read::read(&mut file, &mut buffer)?;
                buffer.truncate(read);
                Ok((file, buffer))
            }));
        }

        let mut pending = this.pending.take().expect("pending read was just set");
        match Pin::new(&mut pending).poll(cx) {
            Poll::Ready(Ok(Ok((file, buffer)))) => {
                if buffer.is_empty() {
                    return Poll::Ready(Some(Err(anyhow::anyhow!(
                        "cached nar partial ended prematurely"
                    ))));
                }
                this.remaining -= buffer.len() as u64;
                this.file = Some(file);
                Poll::Ready(Some(Ok(Bytes::from(buffer))))
            }
            Poll::Ready(Ok(Err(err))) => Poll::Ready(Some(Err(
                anyhow::Error::new(err).context("failed to read cached nar partial")
            ))),
            Poll::Ready(Err(err)) => {
                Poll::Ready(Some(Err(anyhow::anyhow!("disk read task failed: {err}"))))
            }
            Poll::Pending => {
                this.pending = Some(pending);
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expectation_parses_file_hash_and_size() {
        let content = "\
StorePath: /nix/store/abc-hello\n\
URL: nar/abc.nar.xz\n\
Compression: xz\n\
FileHash: sha256:B13C7F53EA0F76F3F3E3B5DC71B6A4B2CD1E66C1F0B8C2A4D5E6F708192A3B4C\n\
FileSize: 40896\n\
";
        let expectation = NarExpectation::from_nar_info_content(content).unwrap();
        assert_eq!(
            expectation.file_hash,
            "b13c7f53ea0f76f3f3e3b5dc71b6a4b2cd1e66c1f0b8c2a4d5e6f708192a3b4c",
        );
        assert_eq!(expectation.file_size, 40896);
    }

    #[test]
    fn expectation_is_none_given_missing_fields() {
        assert!(
            NarExpectation::from_nar_info_content(
                "StorePath: /nix/store/abc-hello\nURL: nar/abc.nar.xz\n",
            )
            .is_none()
        );
        assert!(NarExpectation::from_nar_info_content(
            "FileHash: sha256:b13c7f53ea0f76f3f3e3b5dc71b6a4b2cd1e66c1f0b8c2a4d5e6f708192a3b4c\n",
        )
        .is_none());
        assert!(NarExpectation::from_nar_info_content(
            "FileHash: sha256:b13c7f53ea0f76f3f3e3b5dc71b6a4b2cd1e66c1f0b8c2a4d5e6f708192a3b4c\nFileSize: not-a-number\n",
        )
        .is_none());
    }

    #[test]
    fn expectation_is_none_given_other_hash_algorithm_or_malformed_hash() {
        assert!(
            NarExpectation::from_nar_info_content("FileHash: sha512:abcdef\nFileSize: 10\n",)
                .is_none()
        );
        assert!(
            NarExpectation::from_nar_info_content("FileHash: sha256:b13c7f53\nFileSize: 10\n",)
                .is_none()
        );
    }

    #[tokio::test]
    async fn disk_stream_reads_exact_total_bytes() {
        let content: Vec<u8> = (0..600_000u32).map(|i| (i % 251) as u8).collect();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.nar");
        std::fs::write(&path, &content).unwrap();

        let file = std::fs::File::open(&path).unwrap();
        let mut stream = DiskNarStream::new(file, content.len() as u64);
        let mut out = Vec::new();
        while let Some(item) = futures::StreamExt::next(&mut stream).await {
            out.extend_from_slice(&item.unwrap());
        }
        assert_eq!(out, content);
    }
}
