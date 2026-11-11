//! Main loop: connect, reconcile, follow watcher hints, check stability, emit.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use futures::StreamExt;
use smb2::ErrorKind;
use smb2::types::SessionId;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval, interval_at};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::health::Metrics;
use crate::ignore::IgnoreSet;
use crate::kafka::Sink;
use crate::paths;
use crate::smb::{self, Hint, SmbConn};
use crate::stability::{Observation, Pending, Verdict};
use crate::state::{self, FileEvent, FileState, ListedFile, Source, StateMap};

/// Concurrent in-flight files during emit / baseline writes.
const EMIT_CONCURRENCY: usize = 64;
/// Delay before a reconcile triggered by a watcher overflow (bursts coalesce).
const OVERFLOW_DEBOUNCE: Duration = Duration::from_secs(2);

/// Why a connected session ended.
enum Exit {
    Shutdown,
    /// Connection-level SMB error, formatted for the log.
    Smb(String),
}

impl From<smb2::Error> for Exit {
    fn from(e: smb2::Error) -> Self {
        Exit::Smb(fmt_err(&e))
    }
}

fn fmt_err(e: &smb2::Error) -> String {
    format!("{e} (kind {:?})", e.kind())
}

/// Exponential backoff, 1 s doubling up to `max`.
pub struct Backoff {
    next: Duration,
    max: Duration,
}

impl Backoff {
    pub fn new(max: Duration) -> Self {
        Self {
            next: Duration::from_secs(1),
            max,
        }
    }
    pub fn reset(&mut self) {
        self.next = Duration::from_secs(1);
    }
    pub fn next_delay(&mut self) -> Duration {
        let d = self.next;
        self.next = (self.next * 2).min(self.max);
        d
    }
    /// Sleep for the next delay; `false` if cancelled meanwhile.
    pub async fn wait(&mut self, cancel: &CancellationToken) -> bool {
        let d = self.next_delay();
        tokio::select! {
            _ = cancel.cancelled() => false,
            _ = tokio::time::sleep(d) => true,
        }
    }
}

struct ActiveWatcher {
    rx: mpsc::Receiver<Hint>,
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

impl ActiveWatcher {
    async fn stop(self) {
        self.cancel.cancel();
        let _ = self.task.await;
    }
}

pub struct App {
    cfg: Config,
    ignore: IgnoreSet,
    sink: Sink,
    metrics: Arc<Metrics>,
    state: StateMap,
    pending: Pending,
    /// State writes whose event is already acknowledged but whose state
    /// record failed; retried on every stability tick.
    unsaved_state: HashMap<String, FileState>,
    /// First start with an empty state and `INITIAL_SCAN_EMITS_EVENTS=false`:
    /// the first complete reconciliation only fills the state.
    baseline_only: bool,
    watcher_failures: u32,
    watcher_disabled: bool,
    last_session: Option<SessionId>,
}

impl App {
    pub fn new(
        cfg: Config,
        sink: Sink,
        state: StateMap,
        metrics: Arc<Metrics>,
    ) -> anyhow::Result<Self> {
        let ignore = IgnoreSet::new(&cfg.ignore_patterns)?;
        let baseline_only = state.is_empty() && !cfg.initial_scan_emits_events;
        if baseline_only {
            info!("state is empty: first scan records existing files without emitting events");
        }
        metrics.state_files.set(state.len() as i64);
        Ok(Self {
            pending: Pending::new(cfg.stability_delay),
            watcher_disabled: !cfg.use_watcher,
            cfg,
            ignore,
            sink,
            metrics,
            state,
            unsaved_state: HashMap::new(),
            baseline_only,
            watcher_failures: 0,
            last_session: None,
        })
    }

    pub async fn run(mut self, cancel: CancellationToken) {
        let mut backoff = Backoff::new(self.cfg.backoff_max);
        let mut first = true;
        while !cancel.is_cancelled() {
            if !first {
                self.metrics.smb_reconnects.inc();
            }
            first = false;
            let conn = tokio::select! {
                _ = cancel.cancelled() => break,
                r = SmbConn::connect(&self.cfg, self.last_session) => r,
            };
            let conn = match conn {
                Ok(c) => c,
                Err(e) => {
                    error!(error = %e, "SMB connect failed");
                    if !backoff.wait(&cancel).await {
                        break;
                    }
                    continue;
                }
            };
            self.last_session = Some(conn.session_id);
            match self.session(conn, &cancel, &mut backoff).await {
                Exit::Shutdown => break,
                Exit::Smb(e) => {
                    error!(error = %e, "SMB session failed, reconnecting");
                    if !backoff.wait(&cancel).await {
                        break;
                    }
                }
            }
        }
        info!("shutting down: flushing Kafka producer");
        self.sink.flush(Duration::from_secs(10));
    }

    async fn start_watcher(&mut self, conn: &mut SmbConn) -> Result<Option<ActiveWatcher>, Exit> {
        if self.watcher_disabled {
            return Ok(None);
        }
        match conn.watch(&self.cfg.smb_base_path).await {
            Ok(w) => {
                let (tx, rx) = mpsc::channel(4096);
                let cancel = CancellationToken::new();
                let task = tokio::spawn(smb::run_watcher(
                    w,
                    paths::normalize(&self.cfg.smb_base_path),
                    tx,
                    cancel.clone(),
                ));
                info!("CHANGE_NOTIFY watcher active");
                self.metrics.watcher_active.set(1);
                Ok(Some(ActiveWatcher { rx, cancel, task }))
            }
            Err(e) => {
                self.watcher_failed(&e)?;
                Ok(None)
            }
        }
    }

    /// Count a watcher failure; connection errors end the session.
    fn watcher_failed(&mut self, e: &smb2::Error) -> Result<(), Exit> {
        self.metrics.watcher_active.set(0);
        if matches!(
            e.kind(),
            ErrorKind::ConnectionLost | ErrorKind::SessionExpired | ErrorKind::TimedOut
        ) {
            return Err(Exit::Smb(fmt_err(e)));
        }
        self.watcher_failures += 1;
        if e.kind() == ErrorKind::Unsupported
            || self.watcher_failures >= self.cfg.watcher_max_failures
        {
            self.watcher_disabled = true;
            warn!(error = %e, failures = self.watcher_failures,
                  interval = ?self.cfg.poll_interval,
                  "CHANGE_NOTIFY unavailable, falling back to polling");
        } else {
            warn!(error = %e, failures = self.watcher_failures, "watcher failed, re-arming");
        }
        Ok(())
    }

    fn reconcile_period(&self, watching: bool) -> Duration {
        if watching {
            self.cfg.reconcile_interval
        } else {
            self.cfg.poll_interval
        }
    }

    async fn session(
        &mut self,
        mut conn: SmbConn,
        cancel: &CancellationToken,
        backoff: &mut Backoff,
    ) -> Exit {
        let mut watcher: Option<ActiveWatcher> = None;
        let exit = self
            .session_loop(&mut conn, &mut watcher, cancel, backoff)
            .await;
        if let Some(w) = watcher.take() {
            w.stop().await;
        }
        self.metrics.watcher_active.set(0);
        if matches!(exit, Exit::Shutdown) {
            conn.close().await;
        }
        exit
    }

    async fn session_loop(
        &mut self,
        conn: &mut SmbConn,
        watcher: &mut Option<ActiveWatcher>,
        cancel: &CancellationToken,
        backoff: &mut Backoff,
    ) -> Exit {
        // Arm the watcher before the initial scan so nothing slips in between.
        match self.start_watcher(conn).await {
            Ok(w) => *watcher = w,
            Err(e) => return e,
        }
        if let Err(e) = self.reconcile(conn, cancel).await {
            return e;
        }
        backoff.reset();

        let mut period = self.reconcile_period(watcher.is_some());
        let mut reconcile_tick = interval_at(tokio::time::Instant::now() + period, period);
        reconcile_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let check_every = (self.cfg.stability_delay / 2).max(Duration::from_secs(1));
        let mut stability_tick = interval(check_every);
        stability_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut overflow_at: Option<tokio::time::Instant> = None;

        loop {
            let want_period = self.reconcile_period(watcher.is_some());
            if want_period != period {
                period = want_period;
                reconcile_tick = interval_at(tokio::time::Instant::now() + period, period);
                reconcile_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
            }

            let hint = async {
                match watcher.as_mut() {
                    Some(w) => w.rx.recv().await,
                    None => std::future::pending().await,
                }
            };
            let overflow_sleep = async {
                match overflow_at {
                    Some(t) => tokio::time::sleep_until(t).await,
                    None => std::future::pending().await,
                }
            };

            let step: Result<(), Exit> = tokio::select! {
                _ = cancel.cancelled() => return Exit::Shutdown,
                _ = reconcile_tick.tick() => self.reconcile(conn, cancel).await,
                _ = overflow_sleep => {
                    overflow_at = None;
                    self.reconcile(conn, cancel).await
                }
                _ = stability_tick.tick() => self.check_pending(conn, cancel).await,
                h = hint => match h {
                    Some(Hint::Path(p)) => {
                        self.watcher_failures = 0;
                        self.on_hint(conn, &p, cancel).await
                    }
                    Some(Hint::Overflow) => {
                        self.metrics.watcher_overflows.inc();
                        overflow_at.get_or_insert(tokio::time::Instant::now() + OVERFLOW_DEBOUNCE);
                        Ok(())
                    }
                    Some(Hint::Failed(e)) => {
                        if let Some(w) = watcher.take() { w.stop().await; }
                        match self.watcher_failed(&e) {
                            Err(x) => Err(x),
                            Ok(()) => match self.start_watcher(conn).await {
                                // Events may have been lost while the watcher was down.
                                Ok(w) => { *watcher = w; self.reconcile(conn, cancel).await }
                                Err(x) => Err(x),
                            },
                        }
                    }
                    None => {
                        // Watcher task ended without saying why.
                        if let Some(w) = watcher.take() { w.stop().await; }
                        self.metrics.watcher_active.set(0);
                        Ok(())
                    }
                },
            };
            if let Err(e) = step {
                return e;
            }
        }
    }

    /// Full listing compared against the state.
    async fn reconcile(
        &mut self,
        conn: &mut SmbConn,
        cancel: &CancellationToken,
    ) -> Result<(), Exit> {
        let started = Instant::now();
        // A large tree can take minutes; don't hold up SIGTERM for it.
        let walk = conn.walk(
            &self.cfg.smb_base_path,
            &self.ignore,
            self.cfg.path_case_insensitive,
        );
        let listing = run_cancellable(cancel, walk)
            .await
            .ok_or(Exit::Shutdown)??;
        let took = started.elapsed();
        self.metrics.reconcile_duration.observe(took.as_secs_f64());

        if self.baseline_only {
            self.write_baseline(&listing, cancel).await;
            info!(
                files = listing.len(),
                state = self.state.len(),
                ?took,
                "baseline scan finished"
            );
            return Ok(());
        }

        for k in state::vanished(self.pending.keys(), &listing) {
            debug!(key = %k, "pending file vanished");
            self.pending.remove(&k);
        }
        let now = Instant::now();
        let mut ready = Vec::new();
        let mut new = 0usize;
        for f in state::diff(&listing, &self.state) {
            if !self.pending.contains(&f.key) {
                new += 1;
                self.metrics.files_detected.inc();
            }
            let obs = Observation {
                size: f.size,
                mtime: f.mtime,
            };
            if self
                .pending
                .observe(&f.key, &f.smb_path, obs, Source::Reconcile, now)
                == Verdict::Stable
            {
                ready.push(f.key.clone());
            }
        }
        self.metrics.pending.set(self.pending.len() as i64);
        info!(
            files = listing.len(),
            new,
            pending = self.pending.len(),
            ?took,
            "reconcile finished"
        );
        self.emit(ready, cancel).await;
        Ok(())
    }

    async fn write_baseline(&mut self, listing: &[ListedFile], cancel: &CancellationToken) {
        let now = Utc::now();
        let todo: Vec<(String, FileState)> = state::diff(listing, &self.state)
            .into_iter()
            .map(|f| {
                (
                    f.key.clone(),
                    FileState {
                        size: f.size,
                        mtime: f.mtime,
                        first_seen: now,
                    },
                )
            })
            .collect();
        let sink = &self.sink;
        let results = run_cancellable(
            cancel,
            futures::stream::iter(todo)
                .map(|(k, st)| async move {
                    let r = sink.send_state(&k, &st).await;
                    (k, st, r)
                })
                .buffer_unordered(EMIT_CONCURRENCY)
                .collect::<Vec<_>>(),
        )
        .await;
        let Some(results) = results else { return };
        let mut failed = 0usize;
        for (k, st, r) in results {
            match r {
                Ok(()) => {
                    self.state.insert(k, st);
                }
                Err(e) => {
                    failed += 1;
                    self.metrics.kafka_errors.inc();
                    debug!(key = %k, error = %e, "baseline state write failed");
                }
            }
        }
        self.metrics.state_files.set(self.state.len() as i64);
        if failed == 0 {
            self.baseline_only = false;
        } else {
            error!(
                failed,
                "baseline: some state writes failed, retrying at next reconcile"
            );
        }
    }

    /// A watcher said something changed at `path`.
    async fn on_hint(
        &mut self,
        conn: &mut SmbConn,
        path: &str,
        cancel: &CancellationToken,
    ) -> Result<(), Exit> {
        if self.ignore.is_ignored(path) {
            return Ok(());
        }
        let key = paths::key(path, self.cfg.path_case_insensitive);
        if self.state.contains_key(&key) {
            return Ok(());
        }
        let st = match conn.stat(path).await {
            Ok(Some(s)) => s,
            Ok(None) => return Ok(()),
            Err(e) if smb::is_path_error(&e) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        if st.is_link {
            return Ok(());
        }
        let now = Instant::now();
        let mut ready = Vec::new();
        if st.is_directory {
            // A directory appeared (copied/moved in): its files may not get
            // individual notifications.
            let files = match conn
                .walk(path, &self.ignore, self.cfg.path_case_insensitive)
                .await
            {
                Ok(f) => f,
                Err(e) if smb::is_path_error(&e) => return Ok(()),
                Err(e) => return Err(e.into()),
            };
            for f in state::diff(&files, &self.state) {
                if !self.pending.contains(&f.key) {
                    self.metrics.files_detected.inc();
                }
                let obs = Observation {
                    size: f.size,
                    mtime: f.mtime,
                };
                if self
                    .pending
                    .observe(&f.key, &f.smb_path, obs, Source::Watcher, now)
                    == Verdict::Stable
                {
                    ready.push(f.key.clone());
                }
            }
        } else {
            if !self.pending.contains(&key) {
                self.metrics.files_detected.inc();
                debug!(%path, "watcher: new file");
            }
            let obs = Observation {
                size: st.size,
                mtime: st.mtime,
            };
            if self.pending.observe(&key, path, obs, Source::Watcher, now) == Verdict::Stable {
                ready.push(key);
            }
        }
        self.metrics.pending.set(self.pending.len() as i64);
        self.emit(ready, cancel).await;
        Ok(())
    }

    /// Re-stat pending files whose last observation is old enough.
    async fn check_pending(
        &mut self,
        conn: &mut SmbConn,
        cancel: &CancellationToken,
    ) -> Result<(), Exit> {
        self.retry_unsaved_state().await;
        let now = Instant::now();
        let mut ready = Vec::new();
        for key in self.pending.due(now) {
            let Some(c) = self.pending.get(&key) else {
                continue;
            };
            let (smb_path, source) = (c.smb_path.clone(), c.source);
            match conn.stat(&smb_path).await {
                Ok(Some(st)) if !st.is_directory && !st.is_link => {
                    let obs = Observation {
                        size: st.size,
                        mtime: st.mtime,
                    };
                    if self
                        .pending
                        .observe(&key, &smb_path, obs, source, Instant::now())
                        == Verdict::Stable
                    {
                        ready.push(key);
                    }
                }
                Ok(_) => {
                    debug!(%key, "pending file gone");
                    self.pending.remove(&key);
                }
                Err(e) if smb::is_path_error(&e) => {
                    debug!(%key, error = %e, "pending file not accessible, dropping");
                    self.pending.remove(&key);
                }
                Err(e) => return Err(e.into()),
            }
        }
        self.metrics.pending.set(self.pending.len() as i64);
        self.emit(ready, cancel).await;
        Ok(())
    }

    /// For each stable file: send the event, wait for the ack, then write the state.
    async fn emit(&mut self, keys: Vec<String>, cancel: &CancellationToken) {
        if keys.is_empty() {
            return;
        }
        let share = self.cfg.share_unc();
        let items: Vec<(String, FileEvent)> = keys
            .into_iter()
            .filter_map(|k| {
                let c = self.pending.get(&k)?;
                let ev = FileEvent::created(
                    &share,
                    &c.smb_path,
                    c.last.size,
                    c.last.mtime,
                    Utc::now(),
                    c.source,
                );
                Some((k, ev))
            })
            .collect();
        let sink = &self.sink;
        let results = run_cancellable(
            cancel,
            futures::stream::iter(items)
                .map(|(k, ev)| async move {
                    if let Err(e) = sink.send_event(&k, &ev).await {
                        return (k, ev, Err(e), None);
                    }
                    let st = FileState {
                        size: ev.size,
                        mtime: ev.mtime,
                        first_seen: ev.detected_at,
                    };
                    let r = sink.send_state(&k, &st).await;
                    (k, ev, Ok(()), Some((st, r)))
                })
                .buffer_unordered(EMIT_CONCURRENCY)
                .collect::<Vec<_>>(),
        )
        .await;
        let Some(results) = results else { return };

        for (k, ev, ev_res, st_res) in results {
            if let Err(e) = ev_res {
                // Stays pending; retried on the next stability tick.
                self.metrics.kafka_errors.inc();
                error!(key = %k, error = %e, "sending event failed, will retry");
                continue;
            }
            self.metrics.events_sent.inc();
            info!(path = %ev.path, size = ev.size, source = ev.source.as_str(), "file_created event sent");
            self.pending.remove(&k);
            let (st, r) = st_res.expect("state result present after event ok");
            if let Err(e) = r {
                self.metrics.kafka_errors.inc();
                warn!(key = %k, error = %e, "state write failed after event, will retry");
                self.unsaved_state.insert(k.clone(), st.clone());
            }
            self.state.insert(k, st);
        }
        self.metrics.pending.set(self.pending.len() as i64);
        self.metrics.state_files.set(self.state.len() as i64);
    }

    async fn retry_unsaved_state(&mut self) {
        if self.unsaved_state.is_empty() {
            return;
        }
        let todo: Vec<(String, FileState)> = self.unsaved_state.drain().collect();
        for (k, st) in todo {
            if let Err(e) = self.sink.send_state(&k, &st).await {
                self.metrics.kafka_errors.inc();
                debug!(key = %k, error = %e, "state retry failed");
                self.unsaved_state.insert(k, st);
            }
        }
    }
}

/// Run `fut` unless shutdown is requested first. In-flight Kafka messages are
/// left to the final producer flush; at worst a file's event is sent again
/// after restart (at-least-once).
async fn run_cancellable<T>(cancel: &CancellationToken, fut: impl Future<Output = T>) -> Option<T> {
    tokio::select! {
        _ = cancel.cancelled() => None,
        r = fut => Some(r),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_and_caps() {
        let mut b = Backoff::new(Duration::from_secs(10));
        let d: Vec<u64> = (0..6).map(|_| b.next_delay().as_secs()).collect();
        assert_eq!(d, vec![1, 2, 4, 8, 10, 10]);
        b.reset();
        assert_eq!(b.next_delay(), Duration::from_secs(1));
    }
}
