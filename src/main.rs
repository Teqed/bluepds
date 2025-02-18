use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};

use atrium_api::types::string::Did;
use atrium_crypto::keypair::{Export, Secp256k1Keypair};
use axum::{
    body::Body,
    extract::{FromRef, Request, State},
    http::{HeaderMap, Response, StatusCode, Uri},
    response::IntoResponse,
    routing::get,
    Router,
};
use azure_core::auth::TokenCredential;
use clap::Parser;
use clap_verbosity_flag::{log::LevelFilter, InfoLevel, Verbosity};
use config::AppConfig;
use figment::{providers::Format, Figment};
use firehose::FirehoseProducer;
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqliteConnectOptions, SqlitePool};
use tokio::net::TcpListener;
use tower_http::{cors::CorsLayer, trace::TraceLayer};

use anyhow::{anyhow, bail, Context};
use tracing::{info, warn};

mod auth;
mod config;
mod did;
mod endpoints;
mod error;
mod firehose;
mod plc;
mod storage;

pub type Result<T> = std::result::Result<T, error::Error>;
pub use error::Error;
use uuid::Uuid;

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
    r#"
         __                         __
        /\ \__                     /\ \__
    __  \ \ ,_\  _____   _ __   ___\ \ ,_\   ___
  /'__'\ \ \ \/ /\ '__'\/\''__\/ __'\ \ \/  / __'\
 /\ \L\.\_\ \ \_\ \ \L\ \ \ \//\ \L\ \ \ \_/\ \L\ \
 \ \__/.\_\\ \__\\ \ ,__/\ \_\\ \____/\ \__\ \____/
  \/__/\/_/ \/__/ \ \ \/  \/_/ \/___/  \/__/\/___/
                   \ \_\
                    \/_/


This is an AT Protocol Personal Data Server (aka, an atproto PDS)

Most API routes are under /xrpc/

      Code: https://github.com/DrChat/bluepds
  Protocol: https://atproto.com
    "#
}

/// Service proxy.
///
/// Reference: https://atproto.com/specs/xrpc#service-proxying
async fn service_proxy(
    headers: HeaderMap,
    url: Uri,
    State(skey): State<SigningKey>,
    request: Request<Body>,
) -> Result<Response<Body>> {
    let (did, id) = match headers.get("atproto-proxy") {
        Some(val) => {
            let val =
                std::str::from_utf8(val.as_bytes()).context("proxy header not valid utf-8")?;

            let (did, id) = val.split_once('#').context("invalid proxy header")?;

            let did =
                Did::from_str(did).map_err(|e| anyhow!("atproto proxy not a valid DID: {e}"))?;

            (did, id.to_string())
        }
        None => {
            return Err(Error::with_status(
                StatusCode::NOT_FOUND,
                anyhow!("endpoint not found and no `atproto-proxy` header specified"),
            ));
        }
    };

    // TODO: Use some form of caching just so we don't repeatedly try to resolve a DID.
    let did_doc = did::resolve(did.clone())
        .await
        .context("failed to resolve did document")?;

    let service = match did_doc.service.iter().find(|s| s.id == id) {
        Some(service) => service,
        None => {
            return Err(Error::with_status(
                StatusCode::BAD_REQUEST,
                anyhow!("could not find resolve service #{id}"),
            ))
        }
    };

    let url = service
        .service_endpoint
        .join(url.path())
        .context("failed to construct target url")?;

    // TODO: Mint a bearer token by signing a JSON web token.
    // https://github.com/DavidBuchanan314/millipds/blob/5c7529a739d394e223c0347764f1cf4e8fd69f94/src/millipds/appview_proxy.py#L47-L59

    let r = reqwest::Client::new()
        .request(request.method().clone(), url)
        .body(reqwest::Body::wrap_stream(
            request.into_body().into_data_stream(),
        ))
        .send()
        .await
        .context("failed to send request")?;

    let mut resp = Response::builder().status(r.status());
    *resp.headers_mut().unwrap() = r.headers().clone();

    let resp = resp
        .body(Body::from_stream(r.bytes_stream()))
        .context("failed to construct response")?;

    Ok(resp)
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
    let opts = SqliteConnectOptions::from_str(&config.db)
        .context("failed to parse database options")?
        .create_if_missing(true);
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

    let app = Router::new()
        .route("/", get(index))
        .nest("/xrpc", endpoints::routes().fallback(service_proxy))
        // .layer(RateLimitLayer::new(30, Duration::from_secs(30)))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(AppState {
            cred,
            config,
            db: db.clone(),
            firehose: fhp,
            signing_key: skey,
            rotation_key: rkey,
        });

    info!("listening on {addr}");
    info!("connect to: http://127.0.0.1:{}", addr.port());

    // Determine whether or not this was the first startup (i.e. no accounts exist and no invite codes were created).
    // If so, create an invite code and share it via the console.
    let c = sqlx::query_scalar!(
        r#"
        SELECT
            (SELECT COUNT(*) FROM accounts) + (SELECT COUNT(*) FROM invites)
            AS total_count
        "#
    )
    .fetch_one(&db)
    .await
    .context("failed to query database")?;

    if c == 0 {
        let uuid = Uuid::new_v4().to_string();

        sqlx::query!(
            r#"
            INSERT INTO invites (id, did, count, created_at)
                VALUES (?, NULL, 1, datetime('now'))
            "#,
            uuid,
        )
        .execute(&db)
        .await
        .context("failed to create new invite code")?;

        // N.B: This is a sensitive message, so we're bypassing `tracing` here and
        // logging it directly to console.
        println!("=====================================");
        println!("            FIRST STARTUP            ");
        println!("=====================================");
        println!("Use this code to create an account:");
        println!("{uuid}");
        println!("=====================================");
    }

    axum::serve(listener, app.into_make_service())
        .await
        .context("failed to serve app")
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    // Dispatch out to a separate function without a derive macro to help rust-analyzer along.
    run().await
}
