use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};

use atrium_crypto::keypair::{Did, Export, Secp256k1Keypair};
use axum::{extract::FromRef, response::IntoResponse, routing::get, Router};
use azure_core::auth::TokenCredential;
use clap::Parser;
use clap_verbosity_flag::{log::LevelFilter, InfoLevel, Verbosity};
use config::AppConfig;
use figment::{providers::Format, Figment};
use firehose::FirehoseProducer;
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqliteConnectOptions, SqlitePool};
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;

use anyhow::Context;
use tracing::{info, warn};

mod auth;
mod config;
mod endpoints;
mod error;
mod firehose;

pub type Result<T> = std::result::Result<T, error::Error>;
pub use error::Error;

pub type Db = sqlx::SqlitePool;
pub type Cred = Arc<dyn TokenCredential>;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct KeyData {
    /// Primary signing key for all repo operations.
    skey: Vec<u8>,
    /// Primary signing (rotation) key for all PLC operations.
    rkey: Vec<u8>,
}

#[derive(Clone)]
pub struct SigningKey(Arc<Secp256k1Keypair>);
#[derive(Clone)]
pub struct RotationKey(Arc<Secp256k1Keypair>);

impl std::ops::Deref for SigningKey {
    type Target = Secp256k1Keypair;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::Deref for RotationKey {
    type Target = Secp256k1Keypair;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

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
    cred: Cred,
    db: Db,

    firehose: FirehoseProducer,

    signing_key: SigningKey,
    rotation_key: RotationKey,
}

async fn index() -> impl IntoResponse {
    "hello"
}

async fn run() -> anyhow::Result<()> {
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

    if !args.config.exists() {
        // Throw up a warning if the config file does not exist.
        //
        // This is not fatal because users can specify all configuration settings via
        // the environment, but the most likely scenario here is that a user accidentally
        // omitted the config file for some reason (e.g. forgot to mount it into Docker).
        warn!(
            "configuration file {} does not exist",
            args.config.display()
        );
    }

    // Read and parse the user-provided configuration.
    let config: AppConfig = Figment::new()
        .merge(figment::providers::Toml::file(args.config))
        .merge(figment::providers::Env::prefixed("BLUEPDS_"))
        .extract()
        .context("failed to load configuration")?;

    // Check if crypto keys exist. If not, create new ones.
    let (skey, rkey) = if let Ok(f) = std::fs::File::open(&config.key) {
        let keys: KeyData = serde_ipld_dagcbor::from_reader(std::io::BufReader::new(f))
            .context("failed to deserialize crypto keys")?;

        let skey = Secp256k1Keypair::import(&keys.skey).context("failed to import signing key")?;
        let rkey = Secp256k1Keypair::import(&keys.rkey).context("failed to import rotation key")?;

        (SigningKey(Arc::new(skey)), RotationKey(Arc::new(rkey)))
    } else {
        info!("signing keys not found, generating new ones");

        let skey = Secp256k1Keypair::create(&mut rand::thread_rng());
        let rkey = Secp256k1Keypair::create(&mut rand::thread_rng());

        let keys = KeyData {
            skey: skey.export(),
            rkey: rkey.export(),
        };

        let mut f = std::fs::File::create(&config.key).context("failed to create key file")?;
        serde_ipld_dagcbor::to_writer(&mut f, &keys).context("failed to serialize crypto keys")?;

        (SigningKey(Arc::new(skey)), RotationKey(Arc::new(rkey)))
    };

    tokio::fs::create_dir_all(&config.repo.path).await?;
    tokio::fs::create_dir_all(&config.plc.path).await?;

    let cred =
        azure_identity::create_default_credential().context("failed to create Azure credential")?;
    let opts =
        SqliteConnectOptions::from_str(&config.db).context("failed to parse database options")?;
    let db = SqlitePool::connect_with(opts).await?;

    sqlx::migrate!()
        .run(&db)
        .await
        .context("failed to apply migrations")?;

    let (_fh, fhp) = firehose::spawn().await;

    let addr = config
        .listen_address
        .clone()
        .unwrap_or(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000));
    let listener = TcpListener::bind(&addr)
        .await
        .context("failed to bind address")?;

    // Required endpoints:
    // UG /xrpc/_health (undocumented, but impl by reference PDS)
    //
    // com.atproto.identity:
    //   - Table: Resolve handle -> DID
    // UG /xrpc/com.atproto.identity.resolveHandle
    // UP /xrpc/com.atproto.identity.updateHandle
    //
    // com.atproto.server:
    //   - Table: Invitation code expiration list
    //   - Table: Private account details by DID (e.g. password, email)
    // UG /xrpc/com.atproto.server.describeServer
    // UP /xrpc/com.atproto.server.createAccount
    // AP /xrpc/com.atproto.server.createInviteCode
    // UP /xrpc/com.atproto.server.createSession
    // UG /xrpc/com.atproto.server.getSession
    //
    // AP /xrpc/com.atproto.repo.applyWrites
    // AP /xrpc/com.atproto.repo.createRecord
    // AP /xrpc/com.atproto.repo.putRecord
    // AP /xrpc/com.atproto.repo.deleteRecord
    // UG /xrpc/com.atproto.repo.describeRepo
    // UG /xrpc/com.atproto.repo.getRecord
    // UG /xrpc/com.atproto.repo.listRecords
    // AP /xrpc/com.atproto.repo.uploadBlob
    //
    // UG /xrpc/com.atproto.sync.getBlob
    // UG /xrpc/com.atproto.sync.getBlocks
    // UG /xrpc/com.atproto.sync.getLatestCommit
    // UG /xrpc/com.atproto.sync.getRecord
    // UG /xrpc/com.atproto.sync.getRepoStatus
    // UG /xrpc/com.atproto.sync.getRepo
    // UG /xrpc/com.atproto.sync.listBlobs
    // UG /xrpc/com.atproto.sync.listRepos
    // UG /xrpc/com.atproto.sync.subscribeRepos

    let app = Router::new()
        .route("/", get(index))
        .nest("/xrpc", endpoints::routes())
        // .layer(RateLimitLayer::new(30, Duration::from_secs(30)))
        .layer(TraceLayer::new_for_http())
        .with_state(AppState {
            cred,
            config,
            db,
            firehose: fhp,
            signing_key: skey,
            rotation_key: rkey,
        });

    info!("listening on {addr}");
    info!("connect to: http://127.0.0.1:{}", addr.port());

    axum::serve(listener, app.into_make_service())
        .await
        .context("failed to serve app")
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    // Dispatch out to a separate function without a derive macro to help rust-analyzer along.
    run().await
}
