//! Per-NAR single-flight coordination for the NAR disk cache (spec §3, §4).
//!
//! At most one appender (a client-driven tee or a background filler) may write to a partial
//! file at any time. The supervisor registry is the serialization point:
//!
//! - The first miss request acquires the "tee" slot and streams upstream bytes to its client
//!   while appending them to disk.
//! - A partial-hit request joins (or spawns) a *filler*: a background task that resumes the
//!   upstream download from the watermark and appends to disk. When several clients wait on
//!   the same NAR they all attach to the same filler; the last waiter leaving cancels it and
//!   the partial is retained at its current watermark (zero backfill, spec §0.3).
//! - Waiters of a tee are notified when it ends (client disconnect / EOF) so they can
//!   re-inspect the disk and take over the append as a filler if needed.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::StreamExt;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::domain::common::passthrough_headers::PassthroughHeaders;
use crate::domain::common::query_parameters::QueryParameters;
use crate::domain::common::url::Url;
use crate::domain::nar_file::build_nar_file_locations;
use crate::domain::nar_file::model::NarFileLocation;
use crate::domain::nar_file::port::{NarStreamOpenAttempt, NarStreamProvider};
use crate::domain::nar_info::model::NarFileName;
use crate::domain::substituter::SubstituterRepository;
use crate::infrastructure::nar_cache::storage::{NarCacheStorage, VerifyOutcome};

/// Outcome of an append/fill episode, broadcast to every waiter. Cloneable so several
/// waiters can each get a serving handle for a verified file.
#[derive(Debug, Clone)]
pub(crate) enum FillResult {
    /// The file is complete and verified; `file` is a read handle already positioned at
    /// byte 0 (the on-disk name has been unlinked). Each waiter clones its own descriptor
    /// before reading.
    Ready { file: Arc<File>, total: u64 },
    /// The appender ended without producing a ready file (tee ended, or filler cancelled);
    /// the caller should re-inspect the disk state.
    Ended,
    /// No upstream could serve a ranged resume (they ignore `Range`); the caller should fall
    /// back to serving a real-time stream.
    FallbackNeeded,
    /// Every substituter answered not-found.
    NotFound,
    /// The fill could not complete; the message describes why. The partial is retained.
    Failed(String),
}

/// Everything a filler task needs; kept behind an `Arc` so spawned tasks are `'static`.
pub(crate) struct FillDeps {
    pub provider: Arc<dyn NarStreamProvider>,
    pub substituter_repository: Arc<dyn SubstituterRepository>,
    pub storage: Arc<NarCacheStorage>,
    pub supervisor: Arc<AppendSupervisor>,
}

/// Description of one fill attempt.
#[derive(Clone)]
pub(crate) struct FillPlan {
    pub nar_file_name: NarFileName,
    /// Expected sha256 of the whole file (hex), from the narinfo `FileHash`.
    pub file_hash: String,
    pub file_size: u64,
    /// Watermark to resume from (0 for full refetches).
    pub start_offset: u64,
    pub upstream_query: Option<QueryParameters>,
    pub headers: PassthroughHeaders,
}

impl FillPlan {
    pub fn name(&self) -> &str {
        self.nar_file_name.value()
    }
}

pub(crate) struct AppendEntry {
    waiters: AtomicUsize,
    token: CancellationToken,
    outcome_tx: watch::Sender<Option<FillResult>>,
}

impl AppendEntry {
    fn new() -> Arc<Self> {
        let (outcome_tx, _outcome_rx) = watch::channel(None);
        Arc::new(Self {
            waiters: AtomicUsize::new(0),
            token: CancellationToken::new(),
            outcome_tx,
        })
    }
}

/// Single-flight registry: `NAR file name -> current appender episode`.
pub struct AppendSupervisor {
    entries: std::sync::Mutex<HashMap<String, Arc<AppendEntry>>>,
}

impl AppendSupervisor {
    pub fn new() -> Self {
        Self {
            entries: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Names with an active (in-flight) append episode; the sweep skips these.
    pub fn active_names(&self) -> HashSet<String> {
        self.entries.lock().unwrap().keys().cloned().collect()
    }

    /// Acquire the exclusive right to append to `name` for a client-driven tee stream.
    /// Returns `None` when another appender already holds the slot (concurrent miss, the
    /// caller just streams real-time without teeing, spec §4).
    pub fn acquire_tee(self: &Arc<Self>, name: &str) -> Option<TeeGuard> {
        let mut entries = self.entries.lock().unwrap();
        if entries.contains_key(name) {
            return None;
        }
        let entry = AppendEntry::new();
        entries.insert(name.to_string(), Arc::clone(&entry));
        Some(TeeGuard {
            supervisor: Arc::clone(self),
            name: name.to_string(),
            entry,
        })
    }

    /// Wait for a fill of `name` to produce a result, spawning the filler when none is
    /// running. Dropping the returned future early (client disconnect) deregisters the
    /// waiter; the last waiter leaving cancels the filler.
    pub(crate) async fn join_fill(
        self: &Arc<Self>,
        plan: &FillPlan,
        deps: Arc<FillDeps>,
    ) -> FillResult {
        let name = plan.name();
        let entry = {
            let mut entries = self.entries.lock().unwrap();
            match entries.get(name) {
                Some(entry) => Arc::clone(entry),
                None => {
                    let entry = AppendEntry::new();
                    entries.insert(name.to_string(), Arc::clone(&entry));
                    tokio::spawn(run_fill(Arc::clone(&entry), plan.clone(), deps));
                    entry
                }
            }
        };

        let _waiter = WaiterGuard::new(Arc::clone(&entry));
        let mut outcome_rx = entry.outcome_tx.subscribe();
        if let Some(result) = outcome_rx.borrow().clone() {
            return result;
        }
        let result = outcome_rx
            .wait_for(|outcome| outcome.is_some())
            .await
            .ok()
            .and_then(|outcome| outcome.clone());
        match result {
            Some(result) => result,
            None => FillResult::Failed("fill outcome channel closed".to_string()),
        }
    }

    fn remove_entry_if_same(&self, name: &str, entry: &Arc<AppendEntry>) {
        let mut entries = self.entries.lock().unwrap();
        if entries
            .get(name)
            .is_some_and(|current| Arc::ptr_eq(current, entry))
        {
            entries.remove(name);
        }
    }
}

/// Releases the tee slot when the teeing client stream ends (EOF, error, or drop) and wakes
/// any waiters so they can re-inspect the disk.
pub(crate) struct TeeGuard {
    supervisor: Arc<AppendSupervisor>,
    name: String,
    entry: Arc<AppendEntry>,
}

impl Drop for TeeGuard {
    fn drop(&mut self) {
        // Publish first, then free the slot: a waiter waking up here either re-inspects the
        // disk (Ended) or spawns a fresh filler once the slot is free.
        self.entry.outcome_tx.send_replace(Some(FillResult::Ended));
        self.supervisor
            .remove_entry_if_same(&self.name, &self.entry);
    }
}

/// Deregisters a fill waiter; the last one out cancels the filler (zero backfill).
struct WaiterGuard {
    entry: Arc<AppendEntry>,
}

impl WaiterGuard {
    fn new(entry: Arc<AppendEntry>) -> Self {
        entry.waiters.fetch_add(1, Ordering::SeqCst);
        Self { entry }
    }
}

impl Drop for WaiterGuard {
    fn drop(&mut self) {
        if self.entry.waiters.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.entry.token.cancel();
        }
    }
}

/// Body of a spawned filler task: one resume attempt, plus at most one full refetch when
/// verification fails (spec §5). The outcome is broadcast to all waiters.
async fn run_fill(entry: Arc<AppendEntry>, plan: FillPlan, deps: Arc<FillDeps>) {
    let result = match fill_once(&plan, &deps, &entry.token, plan.start_offset, None).await {
        FillStep::Ready { file, total } => FillResult::Ready { file, total },
        FillStep::VerifyFailed(bad_source) => {
            tracing::warn!(nar_file = %plan.name(), source = %bad_source, "nar cache refetching from scratch after verification failure");
            let exclude = Some(bad_source);
            match fill_once(&plan, &deps, &entry.token, 0, exclude.as_ref()).await {
                FillStep::Ready { file, total } => FillResult::Ready { file, total },
                FillStep::VerifyFailed(_) => FillResult::Failed(
                    "nar cache refetch failed verification again, giving up".to_string(),
                ),
                FillStep::Cancelled => FillResult::Ended,
                FillStep::NotFound => FillResult::NotFound,
                FillStep::FallbackNeeded => FillResult::FallbackNeeded,
                FillStep::Failed(message) => FillResult::Failed(message),
            }
        }
        FillStep::Cancelled => FillResult::Ended,
        FillStep::NotFound => FillResult::NotFound,
        FillStep::FallbackNeeded => FillResult::FallbackNeeded,
        FillStep::Failed(message) => FillResult::Failed(message),
    };
    // Publish first so late joiners still get the result, then free the slot.
    entry.outcome_tx.send_replace(Some(result));
    deps.supervisor.remove_entry_if_same(plan.name(), &entry);
}

/// Outcome of a single append attempt.
enum FillStep {
    Ready { file: Arc<File>, total: u64 },
    VerifyFailed(Url),
    Cancelled,
    NotFound,
    FallbackNeeded,
    Failed(String),
}

async fn fill_once(
    plan: &FillPlan,
    deps: &FillDeps,
    token: &CancellationToken,
    start_offset: u64,
    exclude: Option<&Url>,
) -> FillStep {
    let name = plan.name();

    let candidates = build_fill_candidates(deps, plan, exclude).await;
    if candidates.is_empty() {
        return FillStep::Failed("no substituter is available for filling".to_string());
    }

    let (outcome, attempts) = deps
        .provider
        .stream_nar_from_offset(&candidates, start_offset, &plan.headers)
        .await;

    let stream = match outcome {
        Ok(Some(stream)) => stream,
        Ok(None) => return FillStep::NotFound,
        Err(err) => {
            // Some substituter holds the file but cannot serve ranged responses; the caller
            // can still serve the client a real-time stream.
            if attempts
                .iter()
                .any(|a| matches!(a, NarStreamOpenAttempt::RangeUnsupported { .. }))
            {
                tracing::warn!(nar_file = %name, %err, "nar cache bypass: no substituter honors range requests");
                return FillStep::FallbackNeeded;
            }
            return FillStep::Failed(format!("failed to open a ranged nar stream: {err}"));
        }
    };

    let source_url = stream.source_url.clone();
    let mut appender = match deps
        .storage
        .resume_append(name, plan.file_size, start_offset)
    {
        Ok(appender) => appender,
        Err(err) => {
            return FillStep::Failed(format!(
                "failed to open the partial file for appending: {err}"
            ));
        }
    };

    let mut stream = Box::pin(stream.inner);
    loop {
        tokio::select! {
            biased;
            _ = token.cancelled() => {
                // Last waiter left: stop gracefully and retain the partial (spec §0.3).
                let watermark = appender.watermark();
                appender.finish(false);
                tracing::info!(nar_file = %name, watermark, "nar cache filler cancelled, partial retained");
                return FillStep::Cancelled;
            }
            item = stream.next() => match item {
                Some(Ok(bytes)) => {
                    if let Err(err) = appender.append(&bytes) {
                        appender.finish(false);
                        return FillStep::Failed(format!("failed to append to the partial file: {err}"));
                    }
                }
                Some(Err(err)) => {
                    let watermark = appender.watermark();
                    appender.finish(false);
                    tracing::info!(nar_file = %name, watermark, "nar cache partial retained after upstream stream error");
                    return FillStep::Failed(format!("upstream stream failed mid-fill: {err}"));
                }
                None => break,
            }
        }
    }

    let watermark = appender.watermark();
    appender.finish(true);
    if watermark != plan.file_size {
        return FillStep::Failed(format!(
            "upstream stream ended at {watermark} bytes, expected {total}",
            total = plan.file_size,
        ));
    }

    match deps
        .storage
        .verify(name, &plan.file_hash, plan.file_size)
        .await
    {
        Ok(VerifyOutcome::Match(file)) => {
            // The file is verified: serve it through open handles and unlink the name. The
            // complete file is one-shot (spec §0.2) — it exists only for current waiters.
            deps.storage.delete(name);
            tracing::info!(nar_file = %name, size = %plan.file_size, source = %source_url, "nar cache verified");
            FillStep::Ready {
                file: Arc::new(file),
                total: plan.file_size,
            }
        }
        Ok(VerifyOutcome::Mismatch { actual }) => {
            tracing::warn!(nar_file = %name, expected = %plan.file_hash, actual, "nar cache verify failed, evicting & refetching");
            deps.storage.delete(name);
            FillStep::VerifyFailed(source_url)
        }
        Err(err) => FillStep::Failed(format!("failed to verify the completed partial: {err}")),
    }
}

async fn build_fill_candidates(
    deps: &FillDeps,
    plan: &FillPlan,
    exclude: Option<&Url>,
) -> Vec<NarFileLocation> {
    let substituters = deps.substituter_repository.query_all_available().await;
    let candidates = build_nar_file_locations(
        &substituters,
        &plan.nar_file_name,
        plan.upstream_query.as_ref(),
    );
    match exclude {
        Some(excluded) => {
            let filtered: Vec<_> = candidates
                .into_iter()
                .filter(|loc| loc.source_url() != excluded)
                .collect();
            // A single-substituter setup has nowhere else to go: retry the same one.
            if filtered.is_empty() {
                build_nar_file_locations(
                    &substituters,
                    &plan.nar_file_name,
                    plan.upstream_query.as_ref(),
                )
            } else {
                filtered
            }
        }
        None => candidates,
    }
}
