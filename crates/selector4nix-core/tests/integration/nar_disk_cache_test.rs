//! Integration tests for the NAR disk cache (spec §9, T1–T6).
//!
//! Each test drives the `NarDiskCache` facade directly with a scripted upstream provider
//! (filler path) and repeatable `OpenUpstream` closures (real-time miss path), asserting on
//! the on-disk layout under `<dir>/partial/` and on the recorded provider calls.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use anyhow::Result as AnyhowResult;
use async_trait::async_trait;
use futures::StreamExt;
use selector4nix_core::AppError;
use selector4nix_core::domain::common::expire_at::ExpireAt;
use selector4nix_core::domain::common::passthrough_headers::PassthroughHeaders;
use selector4nix_core::domain::common::url::Url;
use selector4nix_core::domain::nar_file::NarFileRepository;
use selector4nix_core::domain::nar_file::model::{NarFile, NarFileKey, NarFileLocation};
use selector4nix_core::domain::nar_file::port::NarStreamData;
use selector4nix_core::domain::nar_info::NarInfoRepository;
use selector4nix_core::domain::nar_info::model::{
    NarFileName, NarInfo, NarInfoResolution, ProxyNarInfoData, StorePathHash, UpstreamNarInfoData,
};
use selector4nix_core::domain::substituter::SubstituterRepository;
use selector4nix_core::domain::substituter::model::test_support::make_substituter_meta_with_url_pri;
use selector4nix_core::domain::substituter::model::{Availability, Substituter};
use selector4nix_core::infrastructure::config::NarDiskCacheConfiguration;
use selector4nix_core::infrastructure::metric::NarCacheMetric;
use selector4nix_core::infrastructure::nar_cache::{NarDiskCache, OpenUpstream, ServedNar};
use selector4nix_core::infrastructure::repository::InMemorySubstituterRepository;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::mock::scripted_nar_stream_provider::{
    CallRecord, ScriptedBody, ScriptedNarStreamProvider, chunked_upstream,
};

// ---------------------------------------------------------------------------
// In-memory repositories backing the facade
// ---------------------------------------------------------------------------

struct MemNarFileRepository {
    files: Mutex<HashMap<NarFileKey, NarFile>>,
}

#[async_trait]
impl NarFileRepository for MemNarFileRepository {
    async fn get(&self, key: &NarFileKey) -> AnyhowResult<Option<NarFile>> {
        Ok(self.files.lock().unwrap().get(key).cloned())
    }

    async fn save(&self, nar_file: NarFile) -> AnyhowResult<()> {
        self.files
            .lock()
            .unwrap()
            .insert(nar_file.key().clone(), nar_file);
        Ok(())
    }

    async fn entry_count(&self) -> AnyhowResult<usize> {
        Ok(self.files.lock().unwrap().len())
    }
}

struct MemNarInfoRepository {
    infos: Mutex<HashMap<StorePathHash, NarInfo>>,
}

#[async_trait]
impl NarInfoRepository for MemNarInfoRepository {
    async fn get(&self, hash: &StorePathHash) -> AnyhowResult<Option<NarInfo>> {
        Ok(self.infos.lock().unwrap().get(hash).cloned())
    }

    async fn save(&self, nar_info: NarInfo) -> AnyhowResult<()> {
        self.infos
            .lock()
            .unwrap()
            .insert(nar_info.hash().clone(), nar_info);
        Ok(())
    }

    async fn entry_count(&self) -> AnyhowResult<usize> {
        Ok(self.infos.lock().unwrap().len())
    }
}

// ---------------------------------------------------------------------------
// Test environment
// ---------------------------------------------------------------------------

/// The watermark metadata as persisted on disk (mirrors `PartialMeta`).
#[derive(Debug, Deserialize)]
struct MetaOnDisk {
    expected_total: u64,
    watermark: u64,
    #[serde(default)]
    complete: bool,
}

struct TestEnv {
    cache: Arc<NarDiskCache>,
    provider: Arc<ScriptedNarStreamProvider>,
    dir: tempfile::TempDir,
    key: NarFileKey,
    name_value: String,
    content: Arc<Vec<u8>>,
    a_src: Url,
    b_src: Url,
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

impl TestEnv {
    async fn build(provider: Arc<ScriptedNarStreamProvider>) -> Self {
        let content: Vec<u8> = (0..3000u32).map(|i| (i % 251) as u8).collect();
        let content = Arc::new(content);
        let file_hash = sha256_hex(&content);
        let name = NarFileName::new(format!("{file_hash}.nar")).unwrap();
        let name_value = name.value().to_string();
        let key = NarFileKey::from_file_name(&name);

        // An unrelated store path hash (32 chars); only its narinfo content matters.
        let store_hash_value = format!("{file_hash:.32}");
        let store_hash = StorePathHash::new(store_hash_value.clone()).unwrap();
        let nar_info_content = format!(
            "StorePath: /nix/store/{store_hash_value}-test-pkg\n\
             URL: nar/{name_value}\n\
             FileHash: sha256:{file_hash}\n\
             FileSize: {}\n",
            content.len(),
        );

        let a_url = Url::new("https://cache-a.example.com").unwrap();
        let b_url = Url::new("https://cache-b.example.com").unwrap();
        let a_meta = make_substituter_meta_with_url_pri(&a_url, 10);
        let b_meta = make_substituter_meta_with_url_pri(&b_url, 20);
        // The filler derives candidate URLs from `meta.storage_url()` (default `<url>/nar`);
        // the scripted source URLs must match exactly.
        let a_src = name.with_storage_prefix(a_meta.storage_url());
        let b_src = name.with_storage_prefix(b_meta.storage_url());

        let substituter_repository = Arc::new(InMemorySubstituterRepository::new());
        // Save order fixes candidate order: A (pri 10) before B (pri 20).
        substituter_repository
            .save(Substituter::new(a_meta.clone(), Availability::Normal))
            .await;
        substituter_repository
            .save(Substituter::new(b_meta.clone(), Availability::Normal))
            .await;

        let nar_file = NarFile::new(key.clone()).on_located(
            NarFileLocation::new(a_src.clone(), a_meta.clone(), None),
            ExpireAt::since(SystemTime::now(), Duration::from_secs(3600)),
            Some(store_hash.clone()),
        );
        let nar_file_repository = Arc::new(MemNarFileRepository {
            files: Mutex::new(HashMap::from([(key.clone(), nar_file)])),
        });

        let upstream = UpstreamNarInfoData::new(nar_info_content).unwrap();
        let (proxy, source_url) = ProxyNarInfoData::proxy_by_keep_url(&upstream, &a_meta);
        let nar_info = NarInfo::new(store_hash.clone()).on_resolved(
            NarInfoResolution::Resolved {
                nar_info: proxy,
                substituter: a_meta,
                source_url,
            },
            ExpireAt::since(SystemTime::now(), Duration::from_secs(3600)),
        );
        let nar_info_repository = Arc::new(MemNarInfoRepository {
            infos: Mutex::new(HashMap::from([(store_hash, nar_info)])),
        });

        let dir = tempfile::tempdir().unwrap();
        let config = NarDiskCacheConfiguration {
            enabled: true,
            dir: dir.path().to_path_buf(),
            max_size_bytes: 1 << 30,
            partial_retention_secs: 3600,
            fsync_interval_bytes: 64 << 20,
        };
        let cache = Arc::new(NarDiskCache::new(
            config,
            provider.clone(),
            substituter_repository,
            nar_file_repository,
            nar_info_repository,
            Arc::new(NarCacheMetric::new()),
        ));

        Self {
            cache,
            provider,
            dir,
            key,
            name_value,
            content,
            a_src,
            b_src,
        }
    }

    fn data_path(&self) -> std::path::PathBuf {
        self.dir.path().join("partial").join(&self.name_value)
    }

    fn data_len(&self) -> u64 {
        std::fs::metadata(self.data_path())
            .expect("partial data file should exist")
            .len()
    }

    fn meta(&self) -> MetaOnDisk {
        let meta_path = self
            .dir
            .path()
            .join("partial")
            .join(format!("{}.json", self.name_value));
        let raw = std::fs::read(meta_path).expect("partial meta file should exist");
        serde_json::from_slice(&raw).unwrap()
    }

    /// Drive a miss through the tee path: stream the first `prefix_len` content bytes to
    /// the client, then disconnect. The partial on disk is then a true prefix of the
    /// content (required for later verification). Returns the retained watermark.
    async fn make_partial_via_tee(&self, prefix_len: usize) -> usize {
        let (prefix, tail) = self.content.split_at(prefix_len);
        let open = chunked_upstream(vec![prefix.to_vec(), tail.to_vec()], self.a_src.clone());
        let served = self
            .serve(open)
            .await
            .expect("miss should serve a real-time stream");
        let mut stream = served.data.inner;
        let chunk = stream.next().await.expect("first chunk").unwrap();
        drop(stream);
        chunk.len()
    }

    /// A do-nothing upstream (empty stream) for requests expected to be served elsewhere.
    fn empty_upstream(&self) -> OpenUpstream {
        chunked_upstream(vec![], self.a_src.clone())
    }

    /// `NarDiskCache::serve` with this env's fixed key and empty passthrough headers.
    fn serve(
        &self,
        open_upstream: OpenUpstream,
    ) -> impl Future<Output = Result<ServedNar, AppError>> + '_ {
        self.cache.serve(
            self.key.clone(),
            None,
            PassthroughHeaders::empty(),
            open_upstream,
        )
    }
}

async fn read_all(data: NarStreamData) -> (Vec<u8>, Url) {
    let source_url = data.source_url.clone();
    let mut out = Vec::new();
    let mut stream = data.inner;
    while let Some(item) = stream.next().await {
        out.extend_from_slice(&item.expect("stream chunk"));
    }
    (out, source_url)
}

// ---------------------------------------------------------------------------
// T1: miss interrupted mid-stream keeps the partial at the right watermark
// ---------------------------------------------------------------------------

#[tokio::test]
async fn t1_miss_disconnect_retains_partial_with_watermark() {
    let provider = ScriptedNarStreamProvider::new();
    let env = TestEnv::build(provider).await;

    let watermark = env.make_partial_via_tee(1024).await;

    assert_eq!(watermark, 1024);
    assert_eq!(env.data_len(), 1024);
    let meta = env.meta();
    assert_eq!(meta.expected_total, env.content.len() as u64);
    assert_eq!(meta.watermark, 1024);
    assert!(!meta.complete);
}

// ---------------------------------------------------------------------------
// T2 + T3: partial hit resumes from the watermark, verifies, serves and unlinks
// ---------------------------------------------------------------------------

#[tokio::test]
async fn t2_t3_partial_hit_resumes_from_watermark_and_serves_verified() {
    let provider = ScriptedNarStreamProvider::new();
    let env = TestEnv::build(provider).await;
    env.provider
        .set_script(env.a_src.clone(), ScriptedBody::Bytes(env.content.clone()));

    let watermark = env.make_partial_via_tee(1024).await;
    assert_eq!(watermark, 1024);

    // Second request: the facade must ask upstream for `Range: bytes=1024-` (T2) and,
    // after verification, serve the full bytes from disk and unlink the partial (T3).
    let served = env
        .serve(env.empty_upstream())
        .await
        .expect("partial hit should fill and serve");

    let calls: Vec<CallRecord> = env.provider.calls();
    assert_eq!(calls.len(), 1, "exactly one ranged fill should happen");
    // The filler offers every available substituter in priority order; the mock picks
    // the first scripted one (A).
    assert_eq!(
        calls[0].candidates,
        vec![env.a_src.clone(), env.b_src.clone()]
    );
    assert_eq!(calls[0].offset, 1024, "resume must start at the watermark");

    let (bytes, source_url) = read_all(served.data).await;
    assert_eq!(bytes, *env.content);
    assert!(
        source_url
            .value()
            .starts_with("http://nar-disk-cache.selector4nix.internal/"),
        "verified files are served from disk under the synthetic URL"
    );

    assert!(
        !env.data_path().exists(),
        "the complete file is one-shot: unlinked after verification"
    );
}

// ---------------------------------------------------------------------------
// T4: verification failure evicts and refetches from another substituter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn t4_verify_failure_refetches_from_other_substituter() {
    let provider = ScriptedNarStreamProvider::new();
    let env = TestEnv::build(provider).await;

    let mut bad = (*env.content).clone();
    let last = bad.len() - 1;
    bad[last] ^= 0xff;
    env.provider
        .set_script(env.a_src.clone(), ScriptedBody::Bytes(Arc::new(bad)));
    env.provider
        .set_script(env.b_src.clone(), ScriptedBody::Bytes(env.content.clone()));

    let watermark = env.make_partial_via_tee(1024).await;
    assert_eq!(watermark, 1024);

    let served = env
        .serve(env.empty_upstream())
        .await
        .expect("refetch from the second substituter should succeed");

    let (bytes, _) = read_all(served.data).await;
    assert_eq!(bytes, *env.content);

    let calls = env.provider.calls();
    assert_eq!(calls.len(), 2, "one resume + one full refetch");
    // First attempt: both candidates offered, resume from the watermark at the bad source.
    assert_eq!(
        calls[0].candidates,
        vec![env.a_src.clone(), env.b_src.clone()]
    );
    assert_eq!(calls[0].offset, 1024);
    // Refetch: the bad source is excluded and the fill restarts from byte 0.
    assert_eq!(calls[1].candidates, vec![env.b_src.clone()]);
    assert_eq!(calls[1].offset, 0);

    assert!(
        !env.data_path().exists(),
        "the one-shot verified file is unlinked after serving"
    );
}

// ---------------------------------------------------------------------------
// T5: two concurrent misses — only one tees to disk, both clients are served
// ---------------------------------------------------------------------------

#[tokio::test]
async fn t5_concurrent_misses_single_tee_both_clients_served() {
    let provider = ScriptedNarStreamProvider::new();
    let env = TestEnv::build(provider).await;

    let open_a = chunked_upstream(vec![(*env.content).clone()], env.a_src.clone());
    let open_b = chunked_upstream(vec![(*env.content).clone()], env.a_src.clone());

    let (served_a, served_b) = tokio::join!(env.serve(open_a), env.serve(open_b));
    let served_a = served_a.expect("first client should be served");
    let served_b = served_b.expect("second client should be served");

    let (bytes_a, _) = read_all(served_a.data).await;
    let (bytes_b, _) = read_all(served_b.data).await;
    assert_eq!(bytes_a, *env.content);
    assert_eq!(bytes_b, *env.content);

    // Exactly one appender wrote the file: its length is exactly the total, never doubled.
    assert_eq!(env.data_len(), env.content.len() as u64);
    let meta = env.meta();
    assert_eq!(meta.watermark, env.content.len() as u64);
    assert!(meta.complete);
}

// ---------------------------------------------------------------------------
// T6: the last waiter leaving cancels the filler; no further upstream requests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn t6_last_waiter_disconnect_cancels_filler() {
    let provider = ScriptedNarStreamProvider::new();
    let env = TestEnv::build(provider).await;
    env.provider
        .set_script(env.a_src.clone(), ScriptedBody::Hang);

    let watermark = env.make_partial_via_tee(1024).await;
    assert_eq!(watermark, 1024);

    // Box::pin: dropping the box must actually destroy the serve future (and with it the
    // fill waiter); a stack `pin!` would only drop the borrow and never cancel the filler.
    let mut serve_fut = Box::pin(env.serve(env.empty_upstream()));

    let outcome = tokio::time::timeout(Duration::from_millis(200), serve_fut.as_mut()).await;
    assert!(
        outcome.is_err(),
        "the filler hangs, so the request must stay pending"
    );

    // The only waiter disconnects: the filler must be cancelled and the partial retained.
    drop(serve_fut);
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(
        env.provider.calls().len(),
        1,
        "no further upstream request may happen after cancellation"
    );
    assert_eq!(
        env.data_len(),
        1024,
        "the partial is retained at the pre-fill watermark"
    );

    // The single-flight slot must be free again: a good upstream now completes the fill.
    env.provider
        .set_script(env.a_src.clone(), ScriptedBody::Bytes(env.content.clone()));
    let served = env
        .serve(env.empty_upstream())
        .await
        .expect("the slot was released, the next request should fill successfully");
    let (bytes, _) = read_all(served.data).await;
    assert_eq!(bytes, *env.content);
}

// ---------------------------------------------------------------------------
// T7: two concurrent partial-hit waiters share one verified file without
//     interleaving (dup'ed fds share a cursor; serving must be positional)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn t7_concurrent_partial_hit_waiters_each_receive_full_bytes() {
    let provider = ScriptedNarStreamProvider::new();
    let env = TestEnv::build(provider).await;
    env.provider
        .set_script(env.a_src.clone(), ScriptedBody::Bytes(env.content.clone()));

    let watermark = env.make_partial_via_tee(1024).await;
    assert_eq!(watermark, 1024);

    // Both requests hit the same partial and join the same filler; the verified file is
    // served to each through its own dup'ed descriptor of one fd.
    let (served_a, served_b) = tokio::join!(
        env.serve(env.empty_upstream()),
        env.serve(env.empty_upstream()),
    );
    let served_a = served_a.expect("first waiter should be served");
    let served_b = served_b.expect("second waiter should be served");

    let (bytes_a, _) = read_all(served_a.data).await;
    let (bytes_b, _) = read_all(served_b.data).await;
    assert_eq!(
        bytes_a, *env.content,
        "waiter A must get the full ordered bytes"
    );
    assert_eq!(
        bytes_b, *env.content,
        "waiter B must get the full ordered bytes"
    );
}
