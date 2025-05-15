//! PDS implementation.
mod account_manager;
mod actor_store;
mod auth;
mod config;
mod db;
mod did;
mod endpoints;
mod error;
mod firehose;
mod metrics;
mod mmap;
mod oauth;
mod plc;
mod schema;
#[cfg(test)]
mod tests;

/// HACK: store private user preferences in the PDS.
///
/// We shouldn't have to know about any bsky endpoints to store private user data.
/// This will _very likely_ be changed in the future.
mod actor_endpoints;

use anyhow::{Context as _, anyhow};
use atrium_api::types::string::Did;
use atrium_crypto::keypair::{Export as _, Secp256k1Keypair};
use auth::AuthenticatedUser;
use axum::{
    Router,
    body::Body,
    extract::{FromRef, Request, State},
    http::{self, HeaderMap, Response, StatusCode, Uri},
    response::IntoResponse,
    routing::get,
};
use azure_core::credentials::TokenCredential;
use clap::Parser;
use clap_verbosity_flag::{InfoLevel, Verbosity, log::LevelFilter};
use config::AppConfig;
use db::establish_pool;
use deadpool_diesel::sqlite::Pool;
use diesel::prelude::*;
use diesel_migrations::{EmbeddedMigrations, embed_migrations};
#[expect(clippy::pub_use, clippy::useless_attribute)]
pub use error::Error;
use figment::{Figment, providers::Format as _};
use firehose::FirehoseProducer;
use http_cache_reqwest::{CacheMode, HttpCacheOptions, MokaManager};
use rand::Rng as _;
use serde::{Deserialize, Serialize};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    str::FromStr as _,
    sync::Arc,
};
use tokio::net::TcpListener;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{info, warn};
use uuid::Uuid;

/// The application user agent. Concatenates the package name and version. e.g. `bluepds/0.0.0`.
pub const APP_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

/// Embedded migrations
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");

/// The application-wide result type.
pub type Result<T> = std::result::Result<T, Error>;
/// The reqwest client type with middleware.
pub type Client = reqwest_middleware::ClientWithMiddleware;
/// The Azure credential type.
pub type Cred = Arc<dyn TokenCredential>;

#[expect(
    clippy::arbitrary_source_item_ordering,
    reason = "serialized data might be structured"
)]
#[derive(Serialize, Deserialize, Debug, Clone)]
/// The key data structure.
struct KeyData {
    /// Primary signing key for all repo operations.
    skey: Vec<u8>,
    /// Primary signing (rotation) key for all PLC operations.
    rkey: Vec<u8>,
}

// FIXME: We should use P256Keypair instead. SecP256K1 is primarily used for cryptocurrencies,
// and the implementations of this algorithm are much more limited as compared to P256.
//
// Reference: https://soatok.blog/2022/05/19/guidance-for-choosing-an-elliptic-curve-signature-algorithm-in-2022/
#[derive(Clone)]
/// The signing key for PLC/DID operations.
pub struct SigningKey(Arc<Secp256k1Keypair>);
#[derive(Clone)]
/// The rotation key for PLC operations.
pub struct RotationKey(Arc<Secp256k1Keypair>);

impl std::ops::Deref for SigningKey {
    type Target = Secp256k1Keypair;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SigningKey {
    /// Import from a private key.
    pub fn import(key: &[u8]) -> Result<Self> {
        let key = Secp256k1Keypair::import(key).context("failed to import signing key")?;
        Ok(Self(Arc::new(key)))
    }
}

impl std::ops::Deref for RotationKey {
    type Target = Secp256k1Keypair;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Parser, Debug, Clone)]
/// Command line arguments.
struct Args {
    /// Path to the configuration file
    #[arg(short, long, default_value = "default.toml")]
    config: PathBuf,
    /// The verbosity level.
    #[command(flatten)]
    verbosity: Verbosity<InfoLevel>,
}

struct ActorPools {
    repo: Pool,
    blob: Pool,
}
impl Clone for ActorPools {
    fn clone(&self) -> Self {
        Self {
            repo: self.repo.clone(),
            blob: self.blob.clone(),
        }
    }
}

#[expect(clippy::arbitrary_source_item_ordering, reason = "arbitrary")]
#[derive(Clone, FromRef)]
struct AppState {
    /// The application configuration.
    config: AppConfig,
    /// The Azure credential.
    cred: Cred,
    /// The main database connection pool. Used for common PDS data, like invite codes.
    db: Pool,
    /// Actor-specific database connection pools. Hashed by DID.
    db_actors: std::collections::HashMap<String, ActorPools>,

    /// The HTTP client with middleware.
    client: Client,
    /// The simple HTTP client.
    simple_client: reqwest::Client,
    /// The firehose producer.
    firehose: FirehoseProducer,

    /// The signing key.
    signing_key: SigningKey,
    /// The rotation key.
    rotation_key: RotationKey,
}

/// The index (/) route.
async fn index() -> impl IntoResponse {
    r"
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
    "
}

/// Service proxy.
///
/// Reference: <https://atproto.com/specs/xrpc#service-proxying>
async fn service_proxy(
    uri: Uri,
    user: AuthenticatedUser,
    State(skey): State<SigningKey>,
    State(client): State<reqwest::Client>,
    headers: HeaderMap,
    request: Request<Body>,
) -> Result<Response<Body>> {
    let url_path = uri.path_and_query().context("invalid service proxy url")?;
    let lxm = url_path
        .path()
        .strip_prefix("/")
        .with_context(|| format!("invalid service proxy url prefix: {}", url_path.path()))?;

    let user_did = user.did();
    let (did, id) = match headers.get("atproto-proxy") {
        Some(val) => {
            let val =
                std::str::from_utf8(val.as_bytes()).context("proxy header not valid utf-8")?;

            let (did, id) = val.split_once('#').context("invalid proxy header")?;

            let did =
                Did::from_str(did).map_err(|e| anyhow!("atproto proxy not a valid DID: {e}"))?;

            (did, format!("#{id}"))
        }
        // HACK: Assume the bluesky appview by default.
        None => (
            Did::new("did:web:api.bsky.app".to_owned())
                .expect("service proxy should be a valid DID"),
            "#bsky_appview".to_owned(),
        ),
    };

    let did_doc = did::resolve(&Client::new(client.clone(), []), did.clone())
        .await
        .with_context(|| format!("failed to resolve did document {}", did.as_str()))?;

    let Some(service) = did_doc.service.iter().find(|s| s.id == id) else {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!("could not find resolve service #{id}"),
        ));
    };

    let target_url: url::Url = service
        .service_endpoint
        .join(&format!("/xrpc{url_path}"))
        .context("failed to construct target url")?;

    let exp = (chrono::Utc::now().checked_add_signed(chrono::Duration::minutes(1)))
        .context("should be valid expiration datetime")?
        .timestamp();
    let jti = rand::thread_rng()
        .sample_iter(rand::distributions::Alphanumeric)
        .take(10)
        .map(char::from)
        .collect::<String>();

    // Mint a bearer token by signing a JSON web token.
    // https://github.com/DavidBuchanan314/millipds/blob/5c7529a739d394e223c0347764f1cf4e8fd69f94/src/millipds/appview_proxy.py#L47-L59
    let token = auth::sign(
        &skey,
        "JWT",
        &serde_json::json!({
            "iss": user_did.as_str(),
            "aud": did.as_str(),
            "lxm": lxm,
            "exp": exp,
            "jti": jti,
        }),
    )
    .context("failed to sign jwt")?;

    let mut h = HeaderMap::new();
    if let Some(hdr) = request.headers().get("atproto-accept-labelers") {
        drop(h.insert("atproto-accept-labelers", hdr.clone()));
    }
    if let Some(hdr) = request.headers().get(http::header::CONTENT_TYPE) {
        drop(h.insert(http::header::CONTENT_TYPE, hdr.clone()));
    }

    let r = client
        .request(request.method().clone(), target_url)
        .headers(h)
        .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
        .body(reqwest::Body::wrap_stream(
            request.into_body().into_data_stream(),
        ))
        .send()
        .await
        .context("failed to send request")?;

    let mut resp = Response::builder().status(r.status());
    if let Some(hdrs) = resp.headers_mut() {
        *hdrs = r.headers().clone();
    }

    let resp = resp
        .body(Body::from_stream(r.bytes_stream()))
        .context("failed to construct response")?;

    Ok(resp)
}

/// The main application entry point.
#[expect(
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    unused_qualifications,
    reason = "main function has high complexity"
)]
async fn run() -> anyhow::Result<()> {
    let args = Args::parse();

    // Set up trace logging to console and account for the user-provided verbosity flag.
    if args.verbosity.log_level_filter() != LevelFilter::Off {
        let lvl = match args.verbosity.log_level_filter() {
            LevelFilter::Error => tracing::Level::ERROR,
            LevelFilter::Warn => tracing::Level::WARN,
            LevelFilter::Info | LevelFilter::Off => tracing::Level::INFO,
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
        .admerge(figment::providers::Toml::file(args.config))
        .admerge(figment::providers::Env::prefixed("BLUEPDS_"))
        .extract()
        .context("failed to load configuration")?;

    if config.test {
        warn!("BluePDS starting up in TEST mode.");
        warn!("This means the application will not federate with the rest of the network.");
        warn!(
            "If you want to turn this off, either set `test` to false in the config or define `BLUEPDS_TEST = false`"
        );
    }

    // Initialize metrics reporting.
    metrics::setup(config.metrics.as_ref()).context("failed to set up metrics exporter")?;

    // Create a reqwest client that will be used for all outbound requests.
    let simple_client = reqwest::Client::builder()
        .user_agent(APP_USER_AGENT)
        .build()
        .context("failed to build requester client")?;
    let client = reqwest_middleware::ClientBuilder::new(simple_client.clone())
        .with(http_cache_reqwest::Cache(http_cache_reqwest::HttpCache {
            mode: CacheMode::Default,
            manager: MokaManager::default(),
            options: HttpCacheOptions::default(),
        }))
        .build();

    tokio::fs::create_dir_all(&config.key.parent().context("should have parent")?)
        .await
        .context("failed to create key directory")?;

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
    tokio::fs::create_dir_all(&config.blob.path).await?;

    let cred = azure_identity::DefaultAzureCredential::new()
        .context("failed to create Azure credential")?;

    // Create a database connection manager and pool for the main database.
    let pool =
        establish_pool(&config.db).context("failed to establish database connection pool")?;
    // Create a dictionary of database connection pools for each actor.
    let mut actor_pools = std::collections::HashMap::new();
    // let mut actor_blob_pools = std::collections::HashMap::new();
    // We'll determine actors by looking in the data/repo dir for .db files.
    let mut actor_dbs = tokio::fs::read_dir(&config.repo.path)
        .await
        .context("failed to read repo directory")?;
    while let Some(entry) = actor_dbs
        .next_entry()
        .await
        .context("failed to read repo dir")?
    {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("db") {
            let did = path
                .file_stem()
                .and_then(|s| s.to_str())
                .context("failed to get actor DID")?;
            let did = Did::from_str(did).expect("should be able to parse actor DID");

            // Create a new database connection manager and pool for the actor.
            // The path for the SQLite connection needs to look like "sqlite://data/repo/<actor>.db"
            let path_repo = format!("sqlite://{}", path.display());
            let actor_repo_pool =
                establish_pool(&path_repo).context("failed to create database connection pool")?;
            // Create a new database connection manager and pool for the actor blobs.
            // The path for the SQLite connection needs to look like "sqlite://data/blob/<actor>.db"
            let path_blob = path_repo.replace("repo", "blob");
            let actor_blob_pool =
                establish_pool(&path_blob).context("failed to create database connection pool")?;
            actor_pools.insert(
                did.to_string(),
                ActorPools {
                    repo: actor_repo_pool,
                    blob: actor_blob_pool,
                },
            );
        }
    }
    // Apply pending migrations
    // let conn = pool.get().await?;
    // conn.run_pending_migrations(MIGRATIONS)
    //     .expect("should be able to run migrations");

    let (_fh, fhp) = firehose::spawn(client.clone(), config.clone());

    let addr = config
        .listen_address
        .unwrap_or(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000));

    let app = Router::new()
        .route("/", get(index))
        .merge(oauth::routes())
        .nest(
            "/xrpc",
            endpoints::routes()
                .merge(actor_endpoints::routes())
                .fallback(service_proxy),
        )
        // .layer(RateLimitLayer::new(30, Duration::from_secs(30)))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(AppState {
            cred,
            config: config.clone(),
            db: pool.clone(),
            db_actors: actor_pools.clone(),
            client: client.clone(),
            simple_client,
            firehose: fhp,
            signing_key: skey,
            rotation_key: rkey,
        });

    info!("listening on {addr}");
    info!("connect to: http://127.0.0.1:{}", addr.port());

    // Determine whether or not this was the first startup (i.e. no accounts exist and no invite codes were created).
    // If so, create an invite code and share it via the console.
    let conn = pool.get().await.context("failed to get db connection")?;

    #[derive(QueryableByName)]
    struct TotalCount {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        total_count: i32,
    }

    // let result = diesel::sql_query(
    //     "SELECT (SELECT COUNT(*) FROM accounts) + (SELECT COUNT(*) FROM invites) AS total_count",
    // )
    // .get_result::<TotalCount>(conn)
    // .context("failed to query database")?;
    let result = conn.interact(move |conn| {
        diesel::sql_query(
            "SELECT (SELECT COUNT(*) FROM accounts) + (SELECT COUNT(*) FROM invites) AS total_count",
        )
        .get_result::<TotalCount>(conn)
    })
    .await
    .expect("should be able to query database")?;

    let c = result.total_count;

    #[expect(clippy::print_stdout)]
    if c == 0 {
        let uuid = Uuid::new_v4().to_string();

        let uuid_clone = uuid.clone();
        conn.interact(move |conn| {
            diesel::sql_query(
            "INSERT INTO invites (id, did, count, created_at) VALUES (?, NULL, 1, datetime('now'))",
        )
        .bind::<diesel::sql_types::Text, _>(uuid_clone)
        .execute(conn)
        .context("failed to create new invite code")
        .expect("should be able to create invite code")
        });

        // N.B: This is a sensitive message, so we're bypassing `tracing` here and
        // logging it directly to console.
        println!("=====================================");
        println!("            FIRST STARTUP            ");
        println!("=====================================");
        println!("Use this code to create an account:");
        println!("{uuid}");
        println!("=====================================");
    }

    let listener = TcpListener::bind(&addr)
        .await
        .context("failed to bind address")?;

    // Serve the app, and request crawling from upstream relays.
    let serve = tokio::spawn(async move {
        axum::serve(listener, app.into_make_service())
            .await
            .context("failed to serve app")
    });

    // Now that the app is live, request a crawl from upstream relays.
    firehose::reconnect_relays(&client, &config).await;

    serve
        .await
        .map_err(Into::into)
        .and_then(|r| r)
        .context("failed to serve app")
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    // Dispatch out to a separate function without a derive macro to help rust-analyzer along.
    run().await
}
