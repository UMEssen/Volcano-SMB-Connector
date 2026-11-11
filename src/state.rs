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
