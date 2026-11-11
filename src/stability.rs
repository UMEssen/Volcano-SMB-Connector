//! Stability check for half-written files.
//!
//! A candidate is "ready" once two observations at least `delay` apart saw the
//! same size and mtime. Observations come from reconciliation listings, watcher
//! hints and the periodic re-stat of pending entries.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};

use crate::state::Source;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observation {
    pub size: u64,
    pub mtime: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct Candidate {
    /// Share-relative SMB path (original case) for re-stat and the event.
    pub smb_path: String,
    /// How the file was first noticed.
    pub source: Source,
    /// Last observed size/mtime.
    pub last: Observation,
    /// When `last` was first seen with these values.
    pub since: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Newly tracked or values changed: wait.
    Waiting,
    /// Unchanged for at least `delay`.
    Stable,
}

#[derive(Debug)]
pub struct Pending {
    delay: Duration,
    entries: HashMap<String, Candidate>,
}

impl Pending {
    pub fn new(delay: Duration) -> Self {
        Self {
            delay,
            entries: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn contains(&self, key: &str) -> bool {
        self.entries.contains_key(key)
    }

    pub fn get(&self, key: &str) -> Option<&Candidate> {
        self.entries.get(key)
    }

    pub fn remove(&mut self, key: &str) -> Option<Candidate> {
        self.entries.remove(key)
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.entries.keys()
    }

    /// Keys whose last observation is old enough to decide on with one more look.
    pub fn due(&self, now: Instant) -> Vec<String> {
        self.entries
            .iter()
            .filter(|(_, c)| now.duration_since(c.since) >= self.delay)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Record an observation and return whether the file is now stable.
    pub fn observe(
        &mut self,
        key: &str,
        smb_path: &str,
        obs: Observation,
        source: Source,
        now: Instant,
    ) -> Verdict {
        match self.entries.get_mut(key) {
            None => {
                self.entries.insert(
                    key.to_string(),
                    Candidate {
                        smb_path: smb_path.to_string(),
                        source,
                        last: obs,
                        since: now,
                    },
                );
                Verdict::Waiting
            }
            Some(c) if c.last != obs => {
                c.last = obs;
                c.since = now;
                c.smb_path = smb_path.to_string();
                Verdict::Waiting
            }
            Some(c) => {
                if now.duration_since(c.since) >= self.delay {
                    Verdict::Stable
                } else {
                    Verdict::Waiting
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs(size: u64, t: i64) -> Observation {
        Observation {
            size,
            mtime: DateTime::from_timestamp(t, 0).unwrap(),
        }
    }

    #[test]
    fn stable_after_delay() {
        let mut p = Pending::new(Duration::from_secs(10));
        let t0 = Instant::now();
        assert_eq!(
            p.observe("a", "a", obs(1, 1), Source::Watcher, t0),
            Verdict::Waiting
        );
        // same values, too soon
        assert_eq!(
            p.observe(
                "a",
                "a",
                obs(1, 1),
                Source::Reconcile,
                t0 + Duration::from_secs(5)
            ),
            Verdict::Waiting
        );
        assert!(p.due(t0 + Duration::from_secs(5)).is_empty());
        assert_eq!(p.due(t0 + Duration::from_secs(10)), vec!["a".to_string()]);
        assert_eq!(
            p.observe(
                "a",
                "a",
                obs(1, 1),
                Source::Reconcile,
                t0 + Duration::from_secs(10)
            ),
            Verdict::Stable
        );
        // first source wins
        assert_eq!(p.get("a").unwrap().source, Source::Watcher);
    }

    #[test]
    fn growing_file_resets() {
        let mut p = Pending::new(Duration::from_secs(10));
        let t0 = Instant::now();
        p.observe("big", "big", obs(100, 1), Source::Watcher, t0);
        // still being written
        assert_eq!(
            p.observe(
                "big",
                "big",
                obs(200, 2),
                Source::Watcher,
                t0 + Duration::from_secs(10)
            ),
            Verdict::Waiting
        );
        // unchanged, but only 5s since last change
        assert_eq!(
            p.observe(
                "big",
                "big",
                obs(200, 2),
                Source::Watcher,
                t0 + Duration::from_secs(15)
            ),
            Verdict::Waiting
        );
        assert_eq!(
            p.observe(
                "big",
                "big",
                obs(200, 2),
                Source::Watcher,
                t0 + Duration::from_secs(20)
            ),
            Verdict::Stable
        );
    }

    #[test]
    fn mtime_change_alone_resets() {
        let mut p = Pending::new(Duration::from_secs(1));
        let t0 = Instant::now();
        p.observe("f", "f", obs(5, 1), Source::Reconcile, t0);
        assert_eq!(
            p.observe(
                "f",
                "f",
                obs(5, 9),
                Source::Reconcile,
                t0 + Duration::from_secs(2)
            ),
            Verdict::Waiting
        );
    }

    #[test]
    fn zero_delay_needs_two_observations() {
        let mut p = Pending::new(Duration::ZERO);
        let t0 = Instant::now();
        assert_eq!(
            p.observe("f", "f", obs(5, 1), Source::Reconcile, t0),
            Verdict::Waiting
        );
        assert_eq!(
            p.observe("f", "f", obs(5, 1), Source::Reconcile, t0),
            Verdict::Stable
        );
    }
}
