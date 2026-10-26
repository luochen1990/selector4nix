//! Disk layout and crash-consistency rules for the NAR disk cache (spec §2, §5, §6).
//!
//! Layout under the configured base directory:
//!
//! ```text
//! <dir>/partial/<nar-file-name>        append-only leading bytes of the NAR
//! <dir>/partial/<nar-file-name>.json   watermark metadata (atomic tmp+rename writes)
//! ```
//!
//! The key is the full NAR file name, which is content-addressed (its base32 hash is the
//! sha256 of the file bytes), so entries are shared across substituters. The effective
//! watermark is `min(file length, meta.watermark)`: a meta lagging behind the data loses its
//! unwritten tail (re-fetched on resume), and data lagging behind the meta (a torn crash)
//! can never serve bytes that were never synced.

use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::infrastructure::nar_cache::sweep::{SweepCandidate, SweepReport, plan_sweep};

/// Result of a whole-file hash check.
pub enum VerifyOutcome {
    /// Hash matches; the file is repositioned at byte 0 for serving.
    Match(File),
    /// Hash differs; `actual` is the computed hex sha256 for diagnostics.
    Mismatch { actual: String },
}

/// Effective watermark: never trust meta beyond what the data file actually holds, and never
/// trust data beyond the last synced meta.
pub fn clamp_watermark(file_len: u64, meta: &PartialMeta) -> u64 {
    file_len.min(meta.watermark)
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Lowercase hex encoding without pulling in an extra dependency.
fn to_lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

/// Persisted watermark metadata. Written atomically (tmp + rename); fields other than
/// `expected_total` default when absent so older writers' files stay readable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartialMeta {
    pub expected_total: u64,
    #[serde(default)]
    pub watermark: u64,
    #[serde(default)]
    pub complete: bool,
    #[serde(default)]
    pub updated_at: u64,
}

/// Disk state of one cached NAR as observed by the request path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartialState {
    /// No usable partial (nothing on disk, corrupt meta, or watermark beyond the expected
    /// total — the latter also deletes the on-disk files).
    Absent,
    /// A prefix of `[0, watermark)` is on disk with `0 < watermark < expected_total`.
    Partial { watermark: u64 },
    /// `watermark == expected_total` bytes are on disk, verified or not.
    Complete { watermark: u64 },
}

/// Append handle for one partial file. Exactly one appender exists per NAR at any time
/// (single-flight, see `supervisor`); writes are buffered by the OS and checkpointed to disk
/// every `fsync_interval_bytes` appended bytes.
pub struct PartialAppender {
    data: File,
    meta: PartialMeta,
    data_path: PathBuf,
    meta_path: PathBuf,
    fsync_interval_bytes: u64,
    bytes_since_checkpoint: u64,
    finished: bool,
}

impl PartialAppender {
    /// Append received bytes and advance the watermark, checkpointing periodically.
    pub fn append(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        if self.finished {
            return Ok(());
        }
        self.data.write_all(bytes)?;
        self.meta.watermark += bytes.len() as u64;
        self.meta.updated_at = unix_now();
        self.bytes_since_checkpoint += bytes.len() as u64;
        if self.bytes_since_checkpoint >= self.fsync_interval_bytes {
            self.checkpoint()?;
        }
        Ok(())
    }

    pub fn watermark(&self) -> u64 {
        self.meta.watermark
    }

    pub fn expected_total(&self) -> u64 {
        self.meta.expected_total
    }

    /// Flush data to disk and persist the watermark meta atomically.
    pub fn checkpoint(&mut self) -> std::io::Result<()> {
        self.data.sync_data()?;
        write_meta_atomic(&self.meta_path, &self.meta)?;
        self.bytes_since_checkpoint = 0;
        Ok(())
    }

    /// Finalize the appender. On graceful EOF (`reached_eof = true`) the complete flag is set
    /// only when the watermark matches the expected total (spec §2); otherwise the partial is
    /// simply retained at its current watermark.
    pub fn finish(&mut self, reached_eof: bool) {
        if self.finished {
            return;
        }
        self.finished = true;
        if reached_eof {
            self.meta.complete = self.meta.watermark == self.meta.expected_total;
        }
        if let Err(err) = self.checkpoint() {
            tracing::warn!(data = %self.data_path.display(), %err, "failed to checkpoint nar cache partial");
        }
    }
}

impl Drop for PartialAppender {
    fn drop(&mut self) {
        // Client disconnects drop the tee stream mid-flight; retain the partial at the last
        // appended watermark (spec §3 miss path).
        self.finish(false);
    }
}

fn write_meta_atomic(meta_path: &Path, meta: &PartialMeta) -> std::io::Result<()> {
    let tmp_path = meta_path.with_extension("json.tmp");
    let content = serde_json::to_string(meta).map_err(|err| {
        std::io::Error::other(format!("could not serialize nar cache meta: {err}"))
    })?;
    {
        let mut tmp = File::create(&tmp_path)?;
        tmp.write_all(content.as_bytes())?;
        tmp.sync_all()?;
    }
    std::fs::rename(&tmp_path, meta_path)
}

/// Filesystem operations for the partial directory. All methods treat missing files as
/// absent state instead of errors (sweep and request paths race on deletions).
pub struct NarCacheStorage {
    partial_dir: PathBuf,
    fsync_interval_bytes: u64,
}

impl NarCacheStorage {
    /// Create the partial directory under `base_dir`. Fails when the directory cannot be
    /// created or written to (the caller bypasses the whole feature in that case).
    pub fn open(base_dir: &Path, fsync_interval_bytes: u64) -> std::io::Result<Self> {
        let partial_dir = base_dir.join("partial");
        std::fs::create_dir_all(&partial_dir)?;
        // Fail fast on read-only mounts instead of failing on every append later.
        let probe = partial_dir.join(".write-probe");
        std::fs::write(&probe, b"")?;
        let _ = std::fs::remove_file(&probe);
        Ok(Self {
            partial_dir,
            fsync_interval_bytes,
        })
    }

    pub fn data_path(&self, name: &str) -> PathBuf {
        self.partial_dir.join(name)
    }

    pub fn meta_path(&self, name: &str) -> PathBuf {
        self.partial_dir.join(format!("{name}.json"))
    }

    fn read_meta(&self, name: &str) -> Option<PartialMeta> {
        let bytes = std::fs::read(self.meta_path(name)).ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    /// Classify the current on-disk state for `name` against `expected_total`.
    pub fn inspect(&self, name: &str, expected_total: u64) -> PartialState {
        let Some(meta) = self.read_meta(name) else {
            return PartialState::Absent;
        };
        let Ok(data_len) = std::fs::metadata(self.data_path(name)).map(|m| m.len()) else {
            return PartialState::Absent;
        };
        let watermark = clamp_watermark(data_len, &meta);
        if watermark > expected_total || meta.expected_total != expected_total {
            // A stale partial of a different (older/newer) object — content-addressed names
            // make this near-impossible, but never resume across mismatched expectations.
            tracing::debug!(
                name,
                watermark,
                expected_total,
                "nar cache partial mismatches expectation, discarding"
            );
            self.delete(name);
            return PartialState::Absent;
        }
        if watermark == expected_total {
            PartialState::Complete { watermark }
        } else {
            PartialState::Partial { watermark }
        }
    }

    /// Start appending a fresh partial from byte 0, replacing any previous content.
    pub fn create_append(
        &self,
        name: &str,
        expected_total: u64,
    ) -> std::io::Result<PartialAppender> {
        self.open_appender(name, expected_total, 0, true)
    }

    /// Resume appending at `watermark`, truncating any torn tail beyond it first.
    pub fn resume_append(
        &self,
        name: &str,
        expected_total: u64,
        watermark: u64,
    ) -> std::io::Result<PartialAppender> {
        self.open_appender(name, expected_total, watermark, false)
    }

    fn open_appender(
        &self,
        name: &str,
        expected_total: u64,
        watermark: u64,
        truncate: bool,
    ) -> std::io::Result<PartialAppender> {
        let data_path = self.data_path(name);
        let meta_path = self.meta_path(name);
        let data = if truncate {
            // Write mode (not append): truncate=true requires the write flag.
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&data_path)?
        } else {
            let data = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&data_path)?;
            // Drop any torn tail beyond the checkpointed watermark before appending.
            data.set_len(watermark)?;
            data
        };
        let meta = PartialMeta {
            expected_total,
            watermark,
            complete: false,
            updated_at: unix_now(),
        };
        write_meta_atomic(&meta_path, &meta)?;
        data.sync_data()?;
        Ok(PartialAppender {
            data,
            meta,
            data_path,
            meta_path,
            fsync_interval_bytes: self.fsync_interval_bytes,
            bytes_since_checkpoint: 0,
            finished: false,
        })
    }

    /// Hash the data file and compare with `expected_hash` (hex sha256).
    pub async fn verify(
        &self,
        name: &str,
        expected_hash: &str,
        expected_total: u64,
    ) -> std::io::Result<VerifyOutcome> {
        let data_path = self.data_path(name);
        let expected_hash = expected_hash.to_ascii_lowercase();
        tokio::task::spawn_blocking(move || {
            let mut file = File::open(&data_path)?;
            if file.metadata()?.len() != expected_total {
                return Ok(VerifyOutcome::Mismatch {
                    actual: "<size mismatch>".to_string(),
                });
            }
            let mut hasher = Sha256::new();
            let mut buffer = vec![0u8; 1024 * 1024];
            loop {
                let read = file.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
            }
            let actual = to_lower_hex(&hasher.finalize());
            if !actual.eq_ignore_ascii_case(&expected_hash) {
                return Ok(VerifyOutcome::Mismatch { actual });
            }
            file.rewind()?;
            Ok(VerifyOutcome::Match(file))
        })
        .await
        .map_err(|err| std::io::Error::other(format!("verify task failed: {err}")))?
    }

    /// Remove the data and meta files for `name`. Idempotent: missing files are ignored
    /// (sweep and request paths race on deletions, spec §6).
    pub fn delete(&self, name: &str) {
        let _ = std::fs::remove_file(self.data_path(name));
        let _ = std::fs::remove_file(self.meta_path(name));
        let _ = std::fs::remove_file(self.meta_path(name).with_extension("json.tmp"));
    }

    /// Execute one sweep pass: delete expired partials, then least-recently-updated ones
    /// until the directory fits `max_size_bytes`. Entries named in `active` (in-flight
    /// appends/fills) are never touched.
    pub fn sweep(
        &self,
        active: &HashSet<String>,
        retention_secs: u64,
        max_size_bytes: u64,
    ) -> SweepReport {
        let now = unix_now();
        let candidates = self.collect_sweep_candidates(active);
        let doomed = plan_sweep(candidates, now, retention_secs, max_size_bytes);
        let mut report = SweepReport::default();
        for name in doomed {
            // Re-check activeness: the entry may have become active after collection.
            if active.contains(&name) {
                continue;
            }
            let reclaimed = self.entry_disk_size(&name);
            self.delete(&name);
            report.deleted += 1;
            report.reclaimed_bytes += reclaimed;
        }
        report
    }

    /// Pair `.json` meta files with their data files and describe every deletable entry.
    /// An entry is a data file name; orphans on either side are reported with size and an
    /// `updated_at` of 0 so the TTL pass removes them.
    fn collect_sweep_candidates(&self, active: &HashSet<String>) -> Vec<SweepCandidate> {
        let entries = match std::fs::read_dir(&self.partial_dir) {
            Ok(entries) => entries,
            Err(_) => return Vec::new(),
        };
        let names: HashSet<String> = entries
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .filter_map(|e| e.file_name().into_string().ok())
            .collect();

        let mut candidates = Vec::new();
        for name in &names {
            // A file is a meta file iff stripping `.json` yields an existing data file;
            // anything else (including crash-leftover `.json.tmp`) counts as data.
            let is_meta = name
                .strip_suffix(".json")
                .is_some_and(|stem| names.contains(stem));
            if is_meta {
                continue;
            }
            if active.contains(name) {
                continue;
            }
            let updated_at = self.read_meta(name).map_or(0, |meta| meta.updated_at);
            candidates.push(SweepCandidate {
                name: name.clone(),
                updated_at,
                size_bytes: self.entry_disk_size(name),
            });
        }
        candidates
    }

    fn entry_disk_size(&self, name: &str) -> u64 {
        let data = std::fs::metadata(self.data_path(name)).map_or(0, |m| m.len());
        let meta = std::fs::metadata(self.meta_path(name)).map_or(0, |m| m.len());
        data + meta
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn appender(storage: &NarCacheStorage, name: &str, total: u64) -> PartialAppender {
        storage.create_append(name, total).unwrap()
    }

    #[test]
    fn clamp_watermark_takes_minimum_of_file_and_meta() {
        let meta = PartialMeta {
            expected_total: 100,
            watermark: 70,
            complete: false,
            updated_at: 1,
        };
        // Meta lags behind data (crash before checkpoint): trust the meta.
        assert_eq!(clamp_watermark(90, &meta), 70);
        // Data lags behind meta (torn write): trust the data.
        assert_eq!(clamp_watermark(60, &meta), 60);
        assert_eq!(clamp_watermark(70, &meta), 70);
    }

    #[test]
    fn meta_deserialization_tolerates_missing_optional_fields() {
        // Meta written by an older writer without watermark/complete/updated_at.
        let meta: PartialMeta = serde_json::from_str(r#"{"expected_total": 4096}"#).unwrap();
        assert_eq!(meta.expected_total, 4096);
        assert_eq!(meta.watermark, 0);
        assert!(!meta.complete);
        assert_eq!(meta.updated_at, 0);
    }

    #[test]
    fn meta_round_trips_through_json() {
        let meta = PartialMeta {
            expected_total: 11,
            watermark: 5,
            complete: false,
            updated_at: 42,
        };
        let text = serde_json::to_string(&meta).unwrap();
        let parsed: PartialMeta = serde_json::from_str(&text).unwrap();
        assert_eq!(parsed, meta);
    }

    #[test]
    fn appender_finish_sets_complete_only_on_matching_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let storage = NarCacheStorage::open(dir.path(), u64::MAX).unwrap();

        let mut a = appender(&storage, "a.nar", 10);
        a.append(&[0u8; 10]).unwrap();
        a.finish(true);
        assert!(storage.read_meta("a.nar").unwrap().complete);

        let mut b = appender(&storage, "b.nar", 10);
        b.append(&[0u8; 4]).unwrap();
        b.finish(true);
        assert!(!storage.read_meta("b.nar").unwrap().complete);
        assert_eq!(storage.read_meta("b.nar").unwrap().watermark, 4);
    }

    #[test]
    fn appender_checkpoint_persists_watermark_periodically() {
        let dir = tempfile::tempdir().unwrap();
        let storage = NarCacheStorage::open(dir.path(), 10).unwrap();

        let mut a = appender(&storage, "a.nar", 100);
        a.append(&[1u8; 4]).unwrap();
        assert_eq!(storage.read_meta("a.nar").unwrap().watermark, 0);
        a.append(&[1u8; 8]).unwrap(); // 12 >= 10 -> checkpoint
        assert_eq!(storage.read_meta("a.nar").unwrap().watermark, 12);
    }

    #[test]
    fn inspect_classifies_states_and_discards_mismatched_expectation() {
        let dir = tempfile::tempdir().unwrap();
        let storage = NarCacheStorage::open(dir.path(), u64::MAX).unwrap();

        assert_eq!(storage.inspect("x.nar", 100), PartialState::Absent);

        let mut a = appender(&storage, "x.nar", 100);
        a.append(&[1u8; 30]).unwrap();
        a.finish(false);
        assert_eq!(
            storage.inspect("x.nar", 100),
            PartialState::Partial { watermark: 30 },
        );

        a = storage.resume_append("x.nar", 100, 30).unwrap();
        a.append(&[1u8; 70]).unwrap();
        a.finish(true);
        assert_eq!(
            storage.inspect("x.nar", 100),
            PartialState::Complete { watermark: 100 },
        );

        // Same on-disk content but a different expectation: discard.
        assert_eq!(storage.inspect("x.nar", 200), PartialState::Absent);
        assert!(!storage.data_path("x.nar").exists());
    }

    #[test]
    fn resume_append_truncates_torn_tail_beyond_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let storage = NarCacheStorage::open(dir.path(), u64::MAX).unwrap();

        let mut a = appender(&storage, "x.nar", 100);
        a.append(&[1u8; 30]).unwrap();
        a.finish(false);
        // Simulate a torn tail: data beyond the checkpointed watermark.
        std::fs::write(storage.data_path("x.nar"), vec![1u8; 50]).unwrap();
        assert_eq!(
            storage.inspect("x.nar", 100),
            PartialState::Partial { watermark: 30 },
        );

        let mut b = storage.resume_append("x.nar", 100, 30).unwrap();
        b.append(&[2u8; 70]).unwrap();
        b.finish(true);
        assert_eq!(
            storage.inspect("x.nar", 100),
            PartialState::Complete { watermark: 100 },
        );
        assert_eq!(
            std::fs::metadata(storage.data_path("x.nar")).unwrap().len(),
            100
        );
    }

    #[tokio::test]
    async fn verify_matches_sha256_and_returns_rewound_file() {
        let dir = tempfile::tempdir().unwrap();
        let storage = NarCacheStorage::open(dir.path(), u64::MAX).unwrap();

        let content = (0..1024u32).map(|i| (i % 251) as u8).collect::<Vec<_>>();
        let hash = to_lower_hex(&Sha256::digest(&content));
        let mut a = appender(&storage, "x.nar", content.len() as u64);
        a.append(&content).unwrap();
        a.finish(true);

        let mut file = match storage
            .verify("x.nar", &hash, content.len() as u64)
            .await
            .unwrap()
        {
            VerifyOutcome::Match(file) => file,
            VerifyOutcome::Mismatch { .. } => panic!("hash should match"),
        };
        let mut first_byte = [0u8; 1];
        file.read_exact(&mut first_byte).unwrap();
        assert_eq!(first_byte[0], content[0]);

        assert!(matches!(
            storage
                .verify("x.nar", "deadbeef", content.len() as u64)
                .await
                .unwrap(),
            VerifyOutcome::Mismatch { .. },
        ));
    }

    #[test]
    fn sweep_deletes_orphans_and_resumes_others_by_ttl_then_lru() {
        let dir = tempfile::tempdir().unwrap();
        let storage = NarCacheStorage::open(dir.path(), u64::MAX).unwrap();
        let now = unix_now();

        let mut fresh = appender(&storage, "fresh.nar", 100);
        fresh.append(&[0u8; 100]).unwrap();
        fresh.finish(false);
        let mut stale = appender(&storage, "stale.nar", 100);
        stale.append(&[0u8; 100]).unwrap();
        stale.finish(false);
        let mut old = storage.create_append("old.nar", 100).unwrap();
        old.append(&[0u8; 100]).unwrap();
        old.finish(false);
        let mut mid = storage.create_append("mid.nar", 100).unwrap();
        mid.append(&[0u8; 100]).unwrap();
        mid.finish(false);

        // Age entries: stale is beyond TTL, old/mid are older/newer within the TTL.
        let age = |name: &str, updated_at: u64| {
            let mut meta = storage.read_meta(name).unwrap();
            meta.updated_at = updated_at;
            write_meta_atomic(&storage.meta_path(name), &meta).unwrap();
        };
        age("fresh.nar", now);
        age("stale.nar", now - 1000);
        age("old.nar", now - 500);
        age("mid.nar", now - 100);
        // Orphan: data file without meta.
        std::fs::write(storage.data_path("orphan.nar"), vec![0u8; 50]).unwrap();
        // Orphan: meta without data.
        write_meta_atomic(
            &storage.meta_path("lonely.nar"),
            &PartialMeta {
                expected_total: 10,
                watermark: 10,
                complete: false,
                updated_at: now,
            },
        )
        .unwrap();

        let report = storage.sweep(&HashSet::new(), 500, u64::MAX);
        assert_eq!(report.deleted, 3); // stale + orphan + lonely
        assert!(!storage.data_path("stale.nar").exists());
        assert!(!storage.data_path("orphan.nar").exists());
        assert!(!storage.meta_path("lonely.nar").exists());
        assert!(storage.data_path("fresh.nar").exists());

        // Size pressure: three ~equal entries (data+meta), budget for two. The oldest
        // (`old.nar`) must go.
        let total: u64 = ["fresh.nar", "old.nar", "mid.nar"]
            .iter()
            .map(|n| storage.entry_disk_size(n))
            .sum();
        let budget = total - storage.entry_disk_size("mid.nar");
        let report = storage.sweep(&HashSet::new(), 500, budget);
        assert_eq!(report.deleted, 1);
        assert!(!storage.data_path("old.nar").exists());
        assert!(storage.data_path("fresh.nar").exists());
        assert!(storage.data_path("mid.nar").exists());
    }

    #[test]
    fn sweep_skips_active_entries() {
        let dir = tempfile::tempdir().unwrap();
        let storage = NarCacheStorage::open(dir.path(), u64::MAX).unwrap();

        let mut a = appender(&storage, "busy.nar", 10);
        a.append(&[0u8; 10]).unwrap();
        a.finish(false);

        let active = HashSet::from(["busy.nar".to_string()]);
        let report = storage.sweep(&active, 0, 0);
        assert_eq!(report.deleted, 0);
        assert!(storage.data_path("busy.nar").exists());
    }
}
