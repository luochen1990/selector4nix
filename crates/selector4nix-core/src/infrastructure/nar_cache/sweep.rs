//! Pure eviction planning for the NAR disk cache sweep (spec §6).
//!
//! Split out of `storage` so the ordering rules are unit-testable without touching the
//! filesystem: first drop entries past their retention deadline, then drop the least
//! recently updated entries until the directory fits the size budget.

/// One deletable partial entry as observed on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepCandidate {
    pub name: String,
    /// Unix seconds of the last watermark progress; `0` for entries without readable meta
    /// (orphans), which makes them TTL-expire first.
    pub updated_at: u64,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweepReport {
    pub deleted: usize,
    pub reclaimed_bytes: u64,
}

/// Decide which entries to delete: everything past the TTL first, then the oldest entries
/// (LRU by `updated_at`) until `total_size - reclaimed <= max_size_bytes`.
pub fn plan_sweep(
    candidates: Vec<SweepCandidate>,
    now_unix: u64,
    retention_secs: u64,
    max_size_bytes: u64,
) -> Vec<String> {
    let total_size: u64 = candidates.iter().map(|c| c.size_bytes).sum();

    let mut expired = Vec::new();
    let mut survivors = Vec::new();
    for candidate in candidates {
        let deadline = candidate.updated_at.saturating_add(retention_secs);
        if deadline < now_unix {
            expired.push(candidate);
        } else {
            survivors.push(candidate);
        }
    }

    // LRU order: oldest first.
    survivors.sort_by_key(|c| c.updated_at);

    let mut doomed: Vec<String> = expired.iter().map(|c| c.name.clone()).collect();
    let mut remaining = total_size - expired.iter().map(|c| c.size_bytes).sum::<u64>();
    for candidate in survivors {
        if remaining <= max_size_bytes {
            break;
        }
        remaining = remaining.saturating_sub(candidate.size_bytes);
        doomed.push(candidate.name);
    }

    doomed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(name: &str, updated_at: u64, size: u64) -> SweepCandidate {
        SweepCandidate {
            name: name.to_string(),
            updated_at,
            size_bytes: size,
        }
    }

    const NOW: u64 = 10_000;

    #[test]
    fn ttl_entries_are_dropped_regardless_of_size_budget() {
        let doomed = plan_sweep(
            vec![
                candidate("stale", NOW - 100, 10),
                candidate("fresh", NOW, 10),
            ],
            NOW,
            50,
            u64::MAX,
        );
        assert_eq!(doomed, vec!["stale".to_string()]);
    }

    #[test]
    fn lru_drops_oldest_first_until_budget_is_met() {
        let doomed = plan_sweep(
            vec![
                candidate("newest", NOW, 100),
                candidate("middle", NOW - 10, 100),
                candidate("oldest", NOW - 20, 100),
            ],
            NOW,
            1_000,
            250,
        );
        assert_eq!(doomed, vec!["oldest".to_string()]);
    }

    #[test]
    fn lru_keeps_everything_when_under_budget() {
        let doomed = plan_sweep(
            vec![candidate("a", NOW - 20, 100), candidate("b", NOW, 100)],
            NOW,
            1_000,
            200,
        );
        assert!(doomed.is_empty());
    }

    #[test]
    fn zero_updated_at_orphans_expire_before_everything_else() {
        // Orphans (updated_at = 0) are both TTL-stale and first in LRU order.
        let doomed = plan_sweep(
            vec![
                candidate("orphan", 0, 10),
                candidate("old", NOW - 20, 10),
                candidate("new", NOW, 10),
            ],
            NOW,
            1_000,
            15,
        );
        assert_eq!(doomed, vec!["orphan".to_string(), "old".to_string()]);
    }
}
