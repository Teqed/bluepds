use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};

use axum::{extract::FromRef, response::IntoResponse, routing::get, Router};
use azure_core::auth::TokenCredential;
use clap::Parser;
use clap_verbosity_flag::{log::LevelFilter, InfoLevel, Verbosity};
use config::AppConfig;
use figment::{providers::Format, Figment};
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;

use anyhow::Context;

mod config;
mod endpoints;
mod error;

pub type Result<T> = std::result::Result<T, error::Error>;

#[derive(Parser, Debug, Clone)]
struct Args {
    #[command(flatten)]
    verbosity: Verbosity<InfoLevel>,

    /// Path to the configuration file
    #[arg(short, long, default_value = "default.toml")]
    config: PathBuf,
}

#[derive(Clone, FromRef)]
struct AppState {
    config: AppConfig,
    cred: Arc<dyn TokenCredential>,
}

async fn index() -> impl IntoResponse {
    "hello"
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Set up trace logging to console and account for the user-provided verbosity flag.
    if args.verbosity.log_level_filter() != LevelFilter::Off {
        let lvl = match args.verbosity.log_level_filter() {
            LevelFilter::Off => tracing::Level::INFO,
            LevelFilter::Error => tracing::Level::ERROR,
            LevelFilter::Warn => tracing::Level::WARN,
            LevelFilter::Info => tracing::Level::INFO,
            LevelFilter::Debug => tracing::Level::DEBUG,
            LevelFilter::Trace => tracing::Level::TRACE,
        };
        tracing_subscriber::fmt().with_max_level(lvl).init();
    }

    // Read and parse the user-provided configuration.
    let config: AppConfig = Figment::new()
        .merge(figment::providers::Toml::file(args.config))
        .merge(figment::providers::Env::prefixed("BLUEPDS_"))
        .extract()
        .context("failed to load configuration")?;

    let cred =
        azure_identity::create_default_credential().context("failed to create Azure credential")?;

    let addr = config
        .listen_address
        .clone()
        .unwrap_or(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000));
    let listener = TcpListener::bind(&addr)
        .await
        .context("failed to bind address")?;

    // Required endpoints:
    // U /xrpc/_health (undocumented, but impl by reference PDS)
    //
    // com.atproto.identity:
    //   - Resolve handle -> DID
    //   - Resolve DID -> repo
    // U /xrpc/com.atproto.identity.resolveHandle
    // U /xrpc/com.atproto.identity.updateHandle
    //
    // U /xrpc/com.atproto.server.describeServer
    // U /xrpc/com.atproto.server.createSession
    // U /xrpc/com.atproto.server.getSession
    //
    // A /xrpc/com.atproto.repo.applyWrites
    // A /xrpc/com.atproto.repo.createRecord
    // A /xrpc/com.atproto.repo.putRecord
    // A /xrpc/com.atproto.repo.deleteRecord
    // U /xrpc/com.atproto.repo.describeRepo
    // U /xrpc/com.atproto.repo.getRecord
    // U /xrpc/com.atproto.repo.listRecords
    // A /xrpc/com.atproto.repo.uploadBlob
    //
    // U /xrpc/com.atproto.sync.getBlob
    // U /xrpc/com.atproto.sync.getBlocks
    // U /xrpc/com.atproto.sync.getLatestCommit
    // U /xrpc/com.atproto.sync.getRecord
    // U /xrpc/com.atproto.sync.getRepoStatus
    // U /xrpc/com.atproto.sync.getRepo
    // U /xrpc/com.atproto.sync.listBlobs
    // U /xrpc/com.atproto.sync.listRepos
    // U /xrpc/com.atproto.sync.subscribeRepos

    let app = Router::new()
        .route("/", get(index))
        .nest("/xrpc", endpoints::routes())
        // .layer(RateLimitLayer::new(30, Duration::from_secs(30)))
        .layer(TraceLayer::new_for_http())
        .with_state(AppState { cred, config });

    tracing::info!("listening on {addr}");
    tracing::info!("connect to: http://127.0.0.1:{}", addr.port());

    axum::serve(listener, app.into_make_service())
        .await
        .context("failed to serve app")
}
