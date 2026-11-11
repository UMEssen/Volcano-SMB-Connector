//! Resumable recursive scan.
//!
//! A [`Scan`] holds the directories still to list and the files found so far.
//! It lives outside the SMB connection, so a scan interrupted by a disconnect
//! continues after the reconnect instead of starting over.
//!
//! Invariant: a directory leaves the queue only after its listing succeeded
//! (or was skipped as unreadable). An error, or a cancelled future in the
//! middle of a listing, leaves it queued.

use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use tracing::{info, warn};

use crate::ignore::IgnoreSet;
use crate::paths;
use crate::smb::{SmbResult, is_path_error};
use crate::state::ListedFile;

const PROGRESS_EVERY: Duration = Duration::from_secs(30);

/// One directory entry, independent of `smb2` types.
#[derive(Debug, Clone)]
pub struct Entry {
    pub name: String,
    pub is_directory: bool,
    pub is_link: bool,
    pub size: u64,
    pub mtime: DateTime<Utc>,
}

#[derive(Debug)]
pub struct Scan {
    root: String,
    queue: Vec<String>,
    files: Vec<ListedFile>,
    dirs_done: u64,
    started: Instant,
    /// How often this scan was interrupted and resumed.
    pub interruptions: u32,
}

impl Scan {
    pub fn new(root: &str) -> Self {
        let root = paths::normalize(root);
        Self {
            queue: vec![root.clone()],
            root,
            files: Vec::new(),
            dirs_done: 0,
            started: Instant::now(),
            interruptions: 0,
        }
    }

    pub fn is_done(&self) -> bool {
        self.queue.is_empty()
    }
    pub fn dirs_done(&self) -> u64 {
        self.dirs_done
    }
    pub fn queued(&self) -> usize {
        self.queue.len()
    }
    pub fn files_found(&self) -> usize {
        self.files.len()
    }
    /// Time since the scan started, including interruptions.
    pub fn age(&self) -> Duration {
        self.started.elapsed()
    }

    /// The complete listing of a finished scan.
    pub fn into_files(self) -> Vec<ListedFile> {
        debug_assert!(self.is_done());
        self.files
    }

    /// List directories until the queue is empty or `list` fails.
    ///
    /// Unreadable sub-directories (deleted meanwhile, no permission) are
    /// skipped with a warning; an error on the root or on the connection is
    /// returned and the scan can be resumed by calling `run` again.
    pub async fn run<L>(
        &mut self,
        mut list: L,
        ignore: &IgnoreSet,
        case_insensitive: bool,
        delay: Duration,
    ) -> SmbResult<()>
    where
        L: AsyncFnMut(&str) -> SmbResult<Vec<Entry>>,
    {
        let mut last_progress = Instant::now();
        let mut first = true;
        while let Some(dir) = self.queue.last().cloned() {
            if !first && !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            first = false;
            if last_progress.elapsed() >= PROGRESS_EVERY {
                last_progress = Instant::now();
                info!(root = %self.root, dirs = self.dirs_done, files = self.files.len(),
                      queued = self.queue.len(), elapsed = ?self.age(), current = %dir,
                      "scan in progress");
            }
            let entries = match list(&dir).await {
                Ok(e) => e,
                Err(e) if dir != self.root && is_path_error(&e) => {
                    warn!(%dir, error = %e, "skipping unreadable directory");
                    self.queue.pop();
                    continue;
                }
                Err(e) => return Err(e),
            };
            // Success: from here on nothing awaits, so the update is atomic
            // with respect to cancellation.
            self.queue.pop();
            self.dirs_done += 1;
            for e in entries {
                if e.name == "." || e.name == ".." || e.is_link {
                    continue;
                }
                let path = paths::join(&dir, &e.name);
                if e.is_directory {
                    self.queue.push(path);
                } else if !ignore.is_ignored(&path) {
                    self.files.push(ListedFile {
                        key: paths::key(&path, case_insensitive),
                        smb_path: path,
                        size: e.size,
                        mtime: e.mtime,
                    });
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smb2::types::Command;
    use smb2::types::status::NtStatus;
    use std::collections::HashMap;

    fn file(name: &str) -> Entry {
        Entry {
            name: name.into(),
            is_directory: false,
            is_link: false,
            size: 1,
            mtime: DateTime::from_timestamp(0, 0).unwrap(),
        }
    }
    fn dir(name: &str) -> Entry {
        Entry {
            is_directory: true,
            ..file(name)
        }
    }

    /// root: a.txt, x/ (b.txt, y/ (c.txt)), z/ (d.txt), link/, ~$tmp
    fn tree() -> HashMap<String, Vec<Entry>> {
        let mut link = dir("link");
        link.is_link = true;
        HashMap::from([
            (
                "".to_string(),
                vec![
                    dir("."),
                    dir(".."),
                    file("a.txt"),
                    dir("x"),
                    dir("z"),
                    link,
                    file("~$tmp"),
                ],
            ),
            ("x".to_string(), vec![file("b.txt"), dir("y")]),
            ("x/y".to_string(), vec![file("c.txt")]),
            ("z".to_string(), vec![file("d.txt")]),
        ])
    }

    fn ignore() -> IgnoreSet {
        IgnoreSet::new(&["~$*".to_string()]).unwrap()
    }

    fn sorted_keys(files: Vec<ListedFile>) -> Vec<String> {
        let mut k: Vec<String> = files.into_iter().map(|f| f.key).collect();
        k.sort();
        k
    }

    fn not_found() -> smb2::Error {
        smb2::Error::Protocol {
            status: NtStatus::OBJECT_NAME_NOT_FOUND,
            command: Command::Create,
        }
    }

    const ALL: [&str; 4] = ["a.txt", "x/b.txt", "x/y/c.txt", "z/d.txt"];

    #[tokio::test]
    async fn full_scan() {
        let t = tree();
        let mut s = Scan::new("");
        s.run(
            async |d: &str| Ok(t[d].clone()),
            &ignore(),
            false,
            Duration::ZERO,
        )
        .await
        .unwrap();
        assert!(s.is_done());
        assert_eq!(s.dirs_done(), 4);
        assert_eq!(sorted_keys(s.into_files()), ALL);
    }

    #[tokio::test]
    async fn resumes_after_connection_errors() {
        let t = tree();
        let mut s = Scan::new("");
        // Fail every second call with a connection error.
        let mut calls = 0;
        let mut failures = 0;
        loop {
            let r = s
                .run(
                    async |d: &str| {
                        calls += 1;
                        if calls % 2 == 0 {
                            Err(smb2::Error::Disconnected)
                        } else {
                            Ok(t[d].clone())
                        }
                    },
                    &ignore(),
                    false,
                    Duration::ZERO,
                )
                .await;
            match r {
                Ok(()) => break,
                Err(_) => failures += 1,
            }
            assert!(failures < 10);
        }
        assert!(failures > 0);
        assert_eq!(s.dirs_done(), 4, "no directory listed twice or lost");
        assert_eq!(sorted_keys(s.into_files()), ALL);
    }

    #[tokio::test]
    async fn cancelled_listing_stays_queued() {
        let t = tree();
        let mut s = Scan::new("");
        // List root, then hang forever on the next directory; cancel via timeout.
        let mut calls = 0;
        let res = tokio::time::timeout(
            Duration::from_millis(50),
            s.run(
                async |d: &str| {
                    calls += 1;
                    if calls == 2 {
                        std::future::pending::<()>().await;
                    }
                    Ok(t[d].clone())
                },
                &ignore(),
                false,
                Duration::ZERO,
            ),
        )
        .await;
        assert!(res.is_err(), "timed out");
        assert_eq!(s.dirs_done(), 1);
        assert_eq!(s.queued(), 2, "x and z still queued");
        s.run(
            async |d: &str| Ok(t[d].clone()),
            &ignore(),
            false,
            Duration::ZERO,
        )
        .await
        .unwrap();
        assert_eq!(sorted_keys(s.into_files()), ALL);
    }

    #[tokio::test]
    async fn vanished_subdir_skipped_root_error_returned() {
        let t = tree();
        let mut s = Scan::new("");
        s.run(
            async |d: &str| {
                if d == "x" {
                    Err(not_found())
                } else {
                    Ok(t[d].clone())
                }
            },
            &ignore(),
            false,
            Duration::ZERO,
        )
        .await
        .unwrap();
        assert_eq!(sorted_keys(s.into_files()), ["a.txt", "z/d.txt"]);

        let mut s = Scan::new("missing");
        let r = s
            .run(
                async |_: &str| Err(not_found()),
                &ignore(),
                false,
                Duration::ZERO,
            )
            .await;
        assert!(r.is_err());
        assert!(!s.is_done(), "root stays queued");
    }
}
