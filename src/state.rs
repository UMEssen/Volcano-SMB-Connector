//! Known-file state, event payloads and the diff between a listing and the state.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

/// Value stored in the compacted `<prefix>.state` topic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileState {
    pub size: u64,
    #[serde(with = "rfc3339")]
    pub mtime: DateTime<Utc>,
    #[serde(with = "rfc3339")]
    pub first_seen: DateTime<Utc>,
}

pub type StateMap = HashMap<String, FileState>;

/// Key of the record that marks the first (event-less) baseline as complete.
///
/// It starts with `/`, which a file key never does ([`crate::paths::key`]
/// strips leading separators), so it cannot collide with a file.
///
/// Why a marker instead of "state is empty": the baseline writes one state
/// record per existing file. If the process dies half-way through those
/// writes, the state is no longer empty but incomplete, and treating every
/// unrecorded old file as new would flood the events topic. Without the
/// marker, the next start finishes the baseline (still without events).
pub const BASELINE_MARKER_KEY: &str = "/baseline-complete";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaselineMarker {
    #[serde(with = "rfc3339")]
    pub completed_at: DateTime<Utc>,
    /// Files recorded in the state when the baseline completed.
    pub files: usize,
}

/// The state topic as loaded at startup.
#[derive(Debug, Default)]
pub struct LoadedState {
    pub files: StateMap,
    pub baseline_complete: bool,
}

impl LoadedState {
    /// Apply one record of the state topic (in offset order).
    /// `payload == None` is a tombstone. Returns an error text for a record
    /// that cannot be parsed (it is skipped).
    pub fn apply(&mut self, key: &str, payload: Option<&[u8]>) -> Result<(), String> {
        if key == BASELINE_MARKER_KEY {
            self.baseline_complete = payload.is_some();
            return Ok(());
        }
        match payload {
            None => {
                self.files.remove(key);
                Ok(())
            }
            Some(p) => match serde_json::from_slice::<FileState>(p) {
                Ok(fs) => {
                    self.files.insert(key.to_string(), fs);
                    Ok(())
                }
                Err(e) => Err(e.to_string()),
            },
        }
    }

    /// Whether the first full scan must only record files, without events.
    pub fn needs_baseline(&self, initial_scan_emits_events: bool) -> bool {
        !self.baseline_complete && !initial_scan_emits_events
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Source {
    Watcher,
    Reconcile,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Source::Watcher => "watcher",
            Source::Reconcile => "reconcile",
        }
    }
}

/// Value written to `<prefix>.events`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileEvent {
    pub event: String,
    pub share: String,
    pub path: String,
    pub size: u64,
    #[serde(with = "rfc3339")]
    pub mtime: DateTime<Utc>,
    #[serde(with = "rfc3339")]
    pub detected_at: DateTime<Utc>,
    pub source: Source,
}

impl FileEvent {
    pub fn created(
        share: &str,
        path: &str,
        size: u64,
        mtime: DateTime<Utc>,
        detected_at: DateTime<Utc>,
        source: Source,
    ) -> Self {
        Self {
            event: "file_created".into(),
            share: share.into(),
            path: path.into(),
            size,
            mtime,
            detected_at,
            source,
        }
    }
}

/// One regular file found while listing the share.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedFile {
    /// Share-relative path, `/`-separated, original case.
    pub smb_path: String,
    /// Normalised key (see [`crate::paths::key`]).
    pub key: String,
    pub size: u64,
    pub mtime: DateTime<Utc>,
}

/// Files in `listing` that the state does not know yet.
pub fn diff<'a>(listing: &'a [ListedFile], state: &StateMap) -> Vec<&'a ListedFile> {
    listing
        .iter()
        .filter(|f| !state.contains_key(&f.key))
        .collect()
}

/// Pending keys that a complete listing no longer contains (deleted or renamed
/// before they became stable).
pub fn vanished<'a>(
    pending: impl Iterator<Item = &'a String>,
    listing: &[ListedFile],
) -> Vec<String> {
    let present: HashSet<&str> = listing.iter().map(|f| f.key.as_str()).collect();
    pending
        .filter(|k| !present.contains(k.as_str()))
        .cloned()
        .collect()
}

/// RFC 3339 with second precision and `Z`, e.g. `2026-11-11T10:15:00Z`.
pub mod rfc3339 {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn format(t: &DateTime<Utc>) -> String {
        t.to_rfc3339_opts(SecondsFormat::AutoSi, true)
    }

    pub fn serialize<S: Serializer>(t: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&format(t))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<DateTime<Utc>, D::Error> {
        let s = String::deserialize(d)?;
        DateTime::parse_from_rfc3339(&s)
            .map(|t| t.with_timezone(&Utc))
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(s, 0).unwrap()
    }

    fn lf(key: &str) -> ListedFile {
        ListedFile {
            smb_path: key.into(),
            key: key.into(),
            size: 1,
            mtime: t(0),
        }
    }

    fn fs_json(size: u64) -> Vec<u8> {
        serde_json::to_vec(&FileState {
            size,
            mtime: t(0),
            first_seen: t(0),
        })
        .unwrap()
    }

    #[test]
    fn load_applies_records_tombstones_and_marker() {
        let mut l = LoadedState::default();
        l.apply("a", Some(&fs_json(1))).unwrap();
        l.apply("b", Some(&fs_json(2))).unwrap();
        l.apply("a", None).unwrap();
        assert!(l.apply("c", Some(b"not json")).is_err());
        assert_eq!(l.files.len(), 1);
        assert_eq!(l.files["b"].size, 2);
        assert!(!l.baseline_complete);

        let marker = serde_json::to_vec(&BaselineMarker {
            completed_at: t(0),
            files: 1,
        })
        .unwrap();
        l.apply(BASELINE_MARKER_KEY, Some(&marker)).unwrap();
        assert!(l.baseline_complete);
        assert!(
            !l.files.contains_key(BASELINE_MARKER_KEY),
            "marker is not a file"
        );
        // A tombstone on the marker withdraws it.
        l.apply(BASELINE_MARKER_KEY, None).unwrap();
        assert!(!l.baseline_complete);
    }

    #[test]
    fn partial_baseline_without_marker_still_needs_baseline() {
        // The crash case: some state records written, no marker.
        let mut l = LoadedState::default();
        l.apply("old/1.svs", Some(&fs_json(1))).unwrap();
        assert!(
            l.needs_baseline(false),
            "must finish the baseline, not emit events"
        );
        assert!(
            !l.needs_baseline(true),
            "INITIAL_SCAN_EMITS_EVENTS=true never baselines"
        );
        l.baseline_complete = true;
        assert!(!l.needs_baseline(false));
        // Empty state on first start: baseline.
        assert!(LoadedState::default().needs_baseline(false));
    }

    #[test]
    fn marker_key_cannot_be_a_file_key() {
        assert_ne!(
            crate::paths::key(BASELINE_MARKER_KEY, false),
            BASELINE_MARKER_KEY
        );
        assert_ne!(
            crate::paths::key(BASELINE_MARKER_KEY, true),
            BASELINE_MARKER_KEY
        );
    }

    #[test]
    fn diff_finds_unknown_only() {
        let listing = vec![lf("a"), lf("b"), lf("c/d")];
        let mut state = StateMap::new();
        state.insert(
            "b".into(),
            FileState {
                size: 1,
                mtime: t(0),
                first_seen: t(0),
            },
        );
        let new: Vec<&str> = diff(&listing, &state)
            .iter()
            .map(|f| f.key.as_str())
            .collect();
        assert_eq!(new, vec!["a", "c/d"]);
    }

    #[test]
    fn diff_ignores_changed_known_files() {
        // Modifications of known files are a non-goal.
        let mut listing = vec![lf("a")];
        listing[0].size = 999;
        let mut state = StateMap::new();
        state.insert(
            "a".into(),
            FileState {
                size: 1,
                mtime: t(0),
                first_seen: t(0),
            },
        );
        assert!(diff(&listing, &state).is_empty());
    }

    #[test]
    fn vanished_pending() {
        let pending = ["a".to_string(), "gone".to_string()];
        let mut v = vanished(pending.iter(), &[lf("a")]);
        v.sort();
        assert_eq!(v, vec!["gone".to_string()]);
    }

    #[test]
    fn json_shapes() {
        let st = FileState {
            size: 5,
            mtime: t(1_793_000_000),
            first_seen: t(1_793_000_012),
        };
        let j = serde_json::to_string(&st).unwrap();
        assert_eq!(
            j,
            r#"{"size":5,"mtime":"2026-10-26T07:33:20Z","first_seen":"2026-10-26T07:33:32Z"}"#
        );
        assert_eq!(serde_json::from_str::<FileState>(&j).unwrap(), st);

        let ev = FileEvent::created(
            r"\\server\share",
            "unterordner/datei.pdf",
            123456,
            t(1_793_000_000),
            t(1_793_000_012),
            Source::Reconcile,
        );
        let v: serde_json::Value = serde_json::to_value(&ev).unwrap();
        assert_eq!(v["event"], "file_created");
        assert_eq!(v["share"], r"\\server\share");
        assert_eq!(v["source"], "reconcile");
        assert_eq!(v["mtime"], "2026-10-26T07:33:20Z");
    }

    #[test]
    fn subsecond_mtime_kept() {
        let ts = DateTime::from_timestamp(1_793_000_000, 500_000_000).unwrap();
        assert_eq!(rfc3339::format(&ts), "2026-10-26T07:33:20.500Z");
    }
}
