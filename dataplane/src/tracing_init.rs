//! OpenTelemetry tracing initialisation for the NovaStor dataplane.
//!
//! Configures a `tracing_subscriber` with an OpenTelemetry layer that exports
//! spans via OTLP/gRPC to a Tempo (or compatible) collector, plus a fmt layer
//! for human-readable console output. Falls back gracefully if the OTel
//! collector is unreachable.

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// Global tracer provider — kept alive so we can flush on shutdown.
static TRACER_PROVIDER: std::sync::OnceLock<SdkTracerProvider> = std::sync::OnceLock::new();

/// Initialise distributed tracing with OpenTelemetry OTLP export.
///
/// Reads `OTEL_EXPORTER_OTLP_ENDPOINT` to configure the gRPC span exporter.
/// If the env var is unset or empty, OTel export is **skipped entirely** to
/// avoid wasting CPU on failed batch-export attempts when no collector is
/// deployed.
///
/// Must be called **after** the tokio runtime is available (the OTLP batch
/// exporter spawns a background task on the current tokio runtime).
pub fn init_tracing(service_name: &str, log_level: &str) {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(log_level));

    // Only enable OTel export when an endpoint is explicitly configured.
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").unwrap_or_default();
    if endpoint.is_empty() {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer())
            .init();
        eprintln!("OpenTelemetry disabled (OTEL_EXPORTER_OTLP_ENDPOINT not set)");
        return;
    }

    // Attempt to build the OTLP exporter + OTel layer.
    match build_otel_provider(service_name, &endpoint) {
        Ok(provider) => {
            let tracer = provider.tracer(service_name.to_string());
            let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
            let _ = TRACER_PROVIDER.set(provider);
            tracing_subscriber::registry()
                .with(env_filter)
                .with(otel_layer)
                .with(tracing_subscriber::fmt::layer())
                .init();
            tracing::info!(
                "OpenTelemetry tracing initialised (OTLP/gRPC → {})",
                endpoint
            );
        }
        Err(e) => {
            // Fall back to fmt-only output.
            tracing_subscriber::registry()
                .with(env_filter)
                .with(tracing_subscriber::fmt::layer())
                .init();
            tracing::warn!(
                "OpenTelemetry init failed, falling back to fmt-only logging: {}",
                e
            );
        }
    }
}

/// Build the OpenTelemetry tracer provider with OTLP/gRPC export.
fn build_otel_provider(
    service_name: &str,
    endpoint: &str,
) -> Result<SdkTracerProvider, Box<dyn std::error::Error + Send + Sync>> {
    use opentelemetry_otlp::{SpanExporter, WithExportConfig};
    use opentelemetry_sdk::Resource;

    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()?;

    let resource = Resource::builder()
        .with_service_name(service_name.to_string())
        .build();

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    Ok(provider)
}

/// Flush all pending spans and shut down the tracer provider.
///
/// Call this before the tokio runtime is dropped to ensure in-flight spans
/// are exported. Safe to call even if OpenTelemetry was never initialised.
pub fn shutdown_tracing() {
    if let Some(provider) = TRACER_PROVIDER.get() {
        if let Err(e) = provider.shutdown() {
            eprintln!("OpenTelemetry shutdown error: {}", e);
        }
    }
}
