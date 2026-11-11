//! Ignore patterns for temporary files.
//!
//! Each pattern is matched against the **file name** (last path component);
//! a pattern containing `/` is matched against the whole share-relative path.
//! Matching is case-insensitive, like Windows file systems.

use anyhow::{Context, Result};
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};

#[derive(Clone)]
pub struct IgnoreSet {
    names: GlobSet,
    paths: GlobSet,
}

impl IgnoreSet {
    pub fn new(patterns: &[String]) -> Result<Self> {
        let mut names = GlobSetBuilder::new();
        let mut paths = GlobSetBuilder::new();
        for p in patterns {
            let glob = GlobBuilder::new(p)
                .case_insensitive(true)
                .literal_separator(true)
                // `~$*` must stay literal: no `{a,b}` alternates or `[...]` surprises
                // beyond what globset does by default.
                .build()
                .with_context(|| format!("invalid ignore pattern {p:?}"))?;
            if p.contains('/') {
                paths.add(glob);
            } else {
                names.add(glob);
            }
        }
        Ok(Self {
            names: names.build()?,
            paths: paths.build()?,
        })
    }

    /// `path` is share-relative and `/`-separated.
    pub fn is_ignored(&self, path: &str) -> bool {
        let name = crate::paths::file_name(path);
        self.names.is_match(name) || self.paths.is_match(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn defaults() -> IgnoreSet {
        let p: Vec<String> = "*.tmp,~$*,.~lock*,Thumbs.db,desktop.ini"
            .split(',')
            .map(String::from)
            .collect();
        IgnoreSet::new(&p).unwrap()
    }

    #[test]
    fn default_patterns() {
        let s = defaults();
        assert!(s.is_ignored("a/b/file.tmp"));
        assert!(s.is_ignored("file.TMP"));
        assert!(s.is_ignored("dir/~$Bericht.docx"));
        assert!(s.is_ignored(".~lock.tabelle.ods#"));
        assert!(s.is_ignored("x/Thumbs.db"));
        assert!(s.is_ignored("x/thumbs.db"));
        assert!(s.is_ignored("Desktop.ini"));
        assert!(!s.is_ignored("dir/Bericht.docx"));
        assert!(!s.is_ignored("tmp/file.pdf"));
        assert!(!s.is_ignored("file.tmp.pdf"));
    }

    #[test]
    fn path_patterns() {
        let s = IgnoreSet::new(&["archiv/**".to_string()]).unwrap();
        assert!(s.is_ignored("archiv/2020/a.pdf"));
        assert!(!s.is_ignored("neu/archiv.pdf"));
    }

    #[test]
    fn empty() {
        let s = IgnoreSet::new(&[]).unwrap();
        assert!(!s.is_ignored("a.tmp"));
    }
}
