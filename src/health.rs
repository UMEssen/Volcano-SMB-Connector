//! `/healthz`, `/readyz` and `/metrics` on one HTTP port.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;
use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use prometheus::{Encoder, Histogram, HistogramOpts, IntCounter, IntGauge, Registry, TextEncoder};
use tokio_util::sync::CancellationToken;
use tracing::info;

pub struct Metrics {
    pub registry: Registry,
    pub files_detected: IntCounter,
    pub events_sent: IntCounter,
    pub kafka_errors: IntCounter,
    pub reconcile_duration: Histogram,
    pub pending: IntGauge,
    pub state_files: IntGauge,
    pub watcher_overflows: IntCounter,
    pub watcher_active: IntGauge,
    pub smb_reconnects: IntCounter,
}

impl Metrics {
    pub fn new() -> Result<Self> {
        let r = Registry::new_custom(Some("smb_watcher".into()), None)?;
        let c = |n: &str, h: &str| -> Result<IntCounter> {
            let m = IntCounter::new(n, h)?;
            r.register(Box::new(m.clone()))?;
            Ok(m)
        };
        let g = |n: &str, h: &str| -> Result<IntGauge> {
            let m = IntGauge::new(n, h)?;
            r.register(Box::new(m.clone()))?;
            Ok(m)
        };
        let reconcile_duration = Histogram::with_opts(
            HistogramOpts::new(
                "reconcile_duration_seconds",
                "Duration of a full reconciliation",
            )
            .buckets(vec![0.1, 0.5, 1.0, 5.0, 15.0, 60.0, 300.0, 900.0]),
        )?;
        r.register(Box::new(reconcile_duration.clone()))?;
        Ok(Self {
            files_detected: c(
                "files_detected_total",
                "New files found (before stability check)",
            )?,
            events_sent: c(
                "events_sent_total",
                "file_created events acknowledged by Kafka",
            )?,
            kafka_errors: c("kafka_errors_total", "Failed Kafka sends")?,
            reconcile_duration,
            pending: g("pending_files", "Files waiting for the stability check")?,
            state_files: g("state_files", "Files known in the state")?,
            watcher_overflows: c("watcher_overflows_total", "CHANGE_NOTIFY overflows")?,
            watcher_active: g("watcher_active", "1 if CHANGE_NOTIFY is in use")?,
            smb_reconnects: c("smb_reconnects_total", "SMB (re)connects after an error")?,
            registry: r,
        })
    }
}

#[derive(Clone)]
struct AppState {
    ready: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
}

pub async fn serve(
    port: u16,
    ready: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
    cancel: CancellationToken,
) -> Result<()> {
    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_handler))
        .with_state(AppState { ready, metrics });
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    info!(port, "health endpoint listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { cancel.cancelled().await })
        .await?;
    Ok(())
}

async fn readyz(State(s): State<AppState>) -> impl IntoResponse {
    if s.ready.load(Ordering::Relaxed) {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "loading state")
    }
}

async fn metrics_handler(State(s): State<AppState>) -> impl IntoResponse {
    let mut buf = Vec::new();
    let enc = TextEncoder::new();
    if let Err(e) = enc.encode(&s.metrics.registry.gather(), &mut buf) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            [(header::CONTENT_TYPE, "text/plain")],
            e.to_string().into_bytes(),
        );
    }
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        buf,
    )
}
