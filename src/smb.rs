//! SMB access on top of the low-level `smb2` API.
//!
//! The low-level API (`Connection` + `Session` + `Tree`) is used for both NTLM
//! and Kerberos because `SmbClient::connect` only does NTLM. Reconnects are
//! handled by the caller: on a connection-level error the whole `SmbConn` is
//! dropped and rebuilt, followed by a full reconciliation.

use chrono::{DateTime, Utc};
use smb2::auth::kerberos::ccache::load_ccache;
use smb2::client::{Connection, Session, Tree};
use smb2::types::SessionId;
use smb2::types::status::NtStatus;
use smb2::{ErrorKind, FileNotifyAction, KerberosCredentials, Watcher};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::{Config, SmbAuth};
use crate::ignore::IgnoreSet;
use crate::paths;
use crate::state::ListedFile;

pub type SmbResult<T> = Result<T, smb2::Error>;

/// Errors that concern one path (deleted while we looked, no permission)
/// rather than the connection.
pub fn is_path_error(e: &smb2::Error) -> bool {
    matches!(
        e.kind(),
        ErrorKind::NotFound
            | ErrorKind::AccessDenied
            | ErrorKind::NotADirectory
            | ErrorKind::IsADirectory
            | ErrorKind::InvalidName
            | ErrorKind::SharingViolation
    )
}

pub fn filetime_utc(ft: smb2::pack::FileTime) -> DateTime<Utc> {
    ft.to_system_time()
        .map(DateTime::<Utc>::from)
        .unwrap_or_default()
}

/// Metadata of a single file.
#[derive(Debug, Clone)]
pub struct Stat {
    pub is_directory: bool,
    pub is_link: bool,
    pub size: u64,
    pub mtime: DateTime<Utc>,
}

pub struct SmbConn {
    conn: Connection,
    tree: Tree,
    pub session_id: SessionId,
}

impl SmbConn {
    pub async fn connect(cfg: &Config, previous: Option<SessionId>) -> SmbResult<Self> {
        let addr = cfg.smb_addr();
        let mut conn = Connection::connect(&addr, cfg.smb_timeout).await?;
        conn.set_require_encryption(cfg.smb_require_encryption);
        conn.set_response_timeout(Some(std::time::Duration::from_secs(60)));
        conn.negotiate().await?;
        if let Some(prev) = previous {
            // Lets the server drop the dead session's handles right away.
            conn.set_previous_session_id(prev);
        }

        let session = match cfg.smb_auth {
            SmbAuth::Ntlm => {
                Session::setup(&mut conn, &cfg.smb_user, &cfg.smb_password, &cfg.smb_domain).await?
            }
            SmbAuth::Kerberos => {
                let creds = KerberosCredentials {
                    username: cfg.smb_user.clone(),
                    password: cfg.smb_password.clone(),
                    realm: cfg.smb_krb_realm.clone(),
                    kdc_address: cfg.smb_krb_kdc.clone(),
                };
                let host = cfg.smb_krb_hostname.as_deref().unwrap_or(cfg.smb_host());
                match &cfg.smb_krb_ccache {
                    Some(path) => {
                        let cc = load_ccache(Some(std::path::Path::new(path)))?;
                        Session::setup_kerberos_from_ccache(&mut conn, &creds, host, &cc).await?
                    }
                    None => Session::setup_kerberos(&mut conn, &creds, host).await?,
                }
            }
        };
        let tree = Tree::connect(&mut conn, &cfg.smb_share).await?;
        info!(server = %addr, share = %cfg.smb_share, session = session.session_id.0, "SMB connected");
        Ok(Self {
            conn,
            tree,
            session_id: session.session_id,
        })
    }

    pub async fn stat(&mut self, path: &str) -> SmbResult<Option<Stat>> {
        match self.tree.stat(&mut self.conn, path).await {
            Ok(fi) => Ok(Some(Stat {
                is_directory: fi.is_directory,
                is_link: fi.is_link(),
                size: fi.size,
                mtime: filetime_utc(fi.modified),
            })),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Recursively list regular files below `dir` (share-relative).
    ///
    /// Links (junctions, symlinks) are not followed. Ignored names are dropped.
    /// A sub-directory that vanishes or is unreadable is skipped with a
    /// warning; an error on `dir` itself or on the connection is returned.
    pub async fn walk(
        &mut self,
        dir: &str,
        ignore: &IgnoreSet,
        case_insensitive: bool,
    ) -> SmbResult<Vec<ListedFile>> {
        let mut out = Vec::new();
        let root = paths::normalize(dir);
        let mut stack = vec![root.clone()];
        while let Some(d) = stack.pop() {
            let entries = match self.tree.list_directory(&mut self.conn, &d).await {
                Ok(e) => e,
                Err(e) if d != root && is_path_error(&e) => {
                    warn!(dir = %d, error = %e, "skipping unreadable directory");
                    continue;
                }
                Err(e) => return Err(e),
            };
            for e in entries {
                if e.name == "." || e.name == ".." || e.is_link() {
                    continue;
                }
                let path = paths::join(&d, &e.name);
                if e.is_directory {
                    stack.push(path);
                } else if !ignore.is_ignored(&path) {
                    out.push(ListedFile {
                        key: paths::key(&path, case_insensitive),
                        smb_path: path,
                        size: e.size,
                        mtime: filetime_utc(e.modified),
                    });
                }
            }
        }
        Ok(out)
    }

    pub async fn watch(&mut self, dir: &str) -> SmbResult<Watcher> {
        self.tree
            .watch(&mut self.conn, &paths::normalize(dir), true)
            .await
    }

    pub async fn close(mut self) {
        if let Err(e) = self.tree.disconnect(&mut self.conn).await {
            debug!(error = %e, "tree disconnect failed");
        }
    }
}

/// Messages from the watcher task to the main loop.
#[derive(Debug)]
pub enum Hint {
    /// Something happened at this share-relative path.
    Path(String),
    /// The server dropped events (STATUS_NOTIFY_ENUM_DIR): do a full reconcile.
    Overflow,
    /// The watcher stopped with an error.
    Failed(smb2::Error),
}

/// Forward CHANGE_NOTIFY events as hints until cancelled or the watcher fails.
pub async fn run_watcher(
    mut watcher: Watcher,
    base: String,
    tx: mpsc::Sender<Hint>,
    cancel: CancellationToken,
) {
    loop {
        let res = tokio::select! {
            _ = cancel.cancelled() => break,
            r = watcher.next_events() => r,
        };
        match res {
            Ok(events) => {
                for ev in events {
                    match ev.action {
                        FileNotifyAction::Added
                        | FileNotifyAction::Modified
                        | FileNotifyAction::RenamedNewName => {
                            let p = paths::join(&base, &ev.filename);
                            if tx.send(Hint::Path(p)).await.is_err() {
                                return;
                            }
                        }
                        _ => {}
                    }
                }
            }
            Err(smb2::Error::Protocol { status, .. }) if status == NtStatus::NOTIFY_ENUM_DIR => {
                warn!("watcher overflow (STATUS_NOTIFY_ENUM_DIR), scheduling full reconcile");
                if tx.send(Hint::Overflow).await.is_err() {
                    return;
                }
            }
            Err(e) => {
                let _ = tx.send(Hint::Failed(e)).await;
                return;
            }
        }
    }
    if let Err(e) = watcher.close().await {
        debug!(error = %e, "watcher close failed");
    }
}
