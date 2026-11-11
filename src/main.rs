//! smb-kafka-watcher: publishes a Kafka event for every new file on an SMB share.

mod app;
mod config;
mod health;
mod ignore;
mod kafka;
mod paths;
mod scan;
mod smb;
mod stability;
mod state;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use crate::app::{App, Backoff};
use crate::config::{Config, LogFormat};

fn init_logging(format: &LogFormat) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,smb2=warn,rdkafka=info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    match format {
        LogFormat::Json => builder.json().flatten_event(true).init(),
        LogFormat::Pretty => builder.init(),
    }
}

async fn shutdown_signal(cancel: CancellationToken) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = term.recv() => info!("SIGTERM received"),
            _ = tokio::signal::ctrl_c() => info!("SIGINT received"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
    cancel.cancel();
}

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("configuration error: {e:#}");
            std::process::exit(2);
        }
    };
    init_logging(&cfg.log_format);
    info!(config = ?cfg, version = env!("CARGO_PKG_VERSION"), "starting smb-kafka-watcher");

    let cancel = CancellationToken::new();
    tokio::spawn(shutdown_signal(cancel.clone()));

    let metrics = Arc::new(health::Metrics::new()?);
    let ready = Arc::new(AtomicBool::new(false));
    let health_task = tokio::spawn({
        let (ready, metrics, cancel) = (ready.clone(), metrics.clone(), cancel.clone());
        let port = cfg.http_port;
        async move {
            if let Err(e) = health::serve(port, ready, metrics, cancel.clone()).await {
                error!(error = %e, "health endpoint failed");
                cancel.cancel();
            }
        }
    });

    // Kafka: topics and state, retried with backoff until it works or we are stopped.
    let mut backoff = Backoff::new(cfg.backoff_max);
    let state = loop {
        let attempt = async {
            kafka::ensure_topics(&cfg).await?;
            kafka::load_state(&cfg).await
        };
        let res = tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            r = attempt => r,
        };
        match res {
            Ok(s) => break s,
            Err(e) => {
                error!(error = format!("{e:#}"), "Kafka startup failed, retrying");
                if !backoff.wait(&cancel).await {
                    return Ok(());
                }
            }
        }
    };
    let sink = kafka::Sink::new(&cfg)?;
    ready.store(true, Ordering::Relaxed);
    info!(files = state.len(), "ready");

    App::new(cfg, sink, state, metrics)?
        .run(cancel.clone())
        .await;
    cancel.cancel();
    let _ = health_task.await;
    info!("stopped");
    Ok(())
}
