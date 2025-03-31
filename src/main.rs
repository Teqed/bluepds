//! PDS implementation.
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    str::FromStr as _,
    sync::Arc,
};

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
use figment::{Figment, providers::Format as _};
use firehose::FirehoseProducer;
use http_cache_reqwest::{CacheMode, HttpCacheOptions, MokaManager};
use rand::Rng as _;
use serde::{Deserialize, Serialize};
use sqlx::{SqlitePool, sqlite::SqliteConnectOptions};
use tokio::net::TcpListener;
use tower_http::{cors::CorsLayer, trace::TraceLayer};

use anyhow::{Context as _, anyhow};
use tracing::{info, warn};

mod auth;
mod config;
mod did;
mod endpoints;
mod error;
mod firehose;
mod metrics;
mod plc;
mod storage;

/// The application-wide result type.
pub type Result<T> = std::result::Result<T, Error>;
pub use error::Error;
use uuid::Uuid;

/// The reqwest client type with middleware.
pub type Client = reqwest_middleware::ClientWithMiddleware;
/// The database connection pool.
pub type Db = SqlitePool;
/// The Azure credential type.
pub type Cred = Arc<dyn TokenCredential>;

/// The application user agent. Concatenates the package name and version. e.g. `bluepds/0.0.0`.
pub const APP_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

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

impl std::ops::Deref for RotationKey {
    type Target = Secp256k1Keypair;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Parser, Debug, Clone)]
/// Command line arguments.
struct Args {
    /// The verbosity level.
    #[command(flatten)]
    verbosity: Verbosity<InfoLevel>,

    /// Path to the configuration file
    #[arg(short, long, default_value = "default.toml")]
    config: PathBuf,
}

#[derive(Clone, FromRef)]
struct AppState {
    /// The application configuration.
    config: AppConfig,
    /// The Azure credential.
    cred: Cred,
    /// The database connection pool.
    db: Db,

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

/// HACK: store private user preferences in the PDS.
///
/// We shouldn't have to know about any bsky endpoints to store private user data.
/// This will _very likely_ be changed in the future.
mod actor_endpoints {
    use atrium_api::app::bsky::actor;
    use axum::{Json, routing::post};
    use constcat::concat;

    use super::*;

    async fn put_preferences(
        user: AuthenticatedUser,
        State(db): State<Db>,
        Json(input): Json<actor::put_preferences::Input>,
    ) -> Result<()> {
        let did = user.did();
        let prefs = sqlx::types::Json(input.preferences.clone());
        _ = sqlx::query!(
            r#"UPDATE accounts SET private_prefs = ? WHERE did = ?"#,
            prefs,
            did
        )
        .execute(&db)
        .await
        .context("failed to update user preferences")?;

        Ok(())
    }

    async fn get_preferences(
        user: AuthenticatedUser,
        State(db): State<Db>,
    ) -> Result<Json<actor::get_preferences::Output>> {
        let did = user.did();
        let json: Option<sqlx::types::Json<actor::defs::Preferences>> =
            sqlx::query_scalar("SELECT private_prefs FROM accounts WHERE did = ?")
                .bind(did)
                .fetch_one(&db)
                .await
                .context("failed to fetch preferences")?;

        if let Some(prefs) = json {
            Ok(Json(
                actor::get_preferences::OutputData {
                    preferences: prefs.0,
                }
                .into(),
            ))
        } else {
            Ok(Json(
                actor::get_preferences::OutputData {
                    preferences: Vec::new(),
                }
                .into(),
            ))
        }
    }

    #[rustfmt::skip]
    /// Register all actor endpoints.
    pub(crate) fn routes() -> Router<AppState> {
        // AP /xrpc/app.bsky.actor.putPreferences
        // AG /xrpc/app.bsky.actor.getPreferences
        Router::new()
            .route(concat!("/", actor::put_preferences::NSID), post(put_preferences))
            .route(concat!("/", actor::get_preferences::NSID),  get(get_preferences))
    }
}

/// Service proxy.
///
/// Reference: https://atproto.com/specs/xrpc#service-proxying
async fn service_proxy(
    url: Uri,
    user: AuthenticatedUser,
    State(skey): State<SigningKey>,
    State(client): State<reqwest::Client>,
    headers: HeaderMap,
    request: Request<Body>,
) -> Result<Response<Body>> {
    let url_path = url.path_and_query().context("invalid service proxy url")?;
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
            Did::new("did:web:api.bsky.app".to_owned()).expect("should be a valid DID"),
            "#bsky_appview".to_owned(),
        ),
    };

    let did_doc = did::resolve(&Client::new(client.clone(), []), did.clone())
        .await
        .with_context(|| format!("failed to resolve did document {}", did.as_str()))?;

    let service = match did_doc.service.iter().find(|s| s.id == id) {
        Some(service) => service,
        None => {
            return Err(Error::with_status(
                StatusCode::BAD_REQUEST,
                anyhow!("could not find resolve service #{id}"),
            ));
        }
    };

    let url = service
        .service_endpoint
        .join(&format!("/xrpc{}", url_path))
        .context("failed to construct target url")?;

    let exp = (chrono::Utc::now() + std::time::Duration::from_secs(60)).timestamp();
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
        serde_json::json!({
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
        .request(request.method().clone(), url)
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
    reason = "main function has high complexity"
)]
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
    metrics::setup(&config.metrics).context("failed to set up metrics exporter")?;

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

    tokio::fs::create_dir_all(&config.key.parent().expect("should have parent"))
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
    let opts = SqliteConnectOptions::from_str(&config.db)
        .context("failed to parse database options")?
        .create_if_missing(true);
    let db = SqlitePool::connect_with(opts).await?;

    sqlx::migrate!()
        .run(&db)
        .await
        .context("failed to apply migrations")?;

    let (_fh, fhp) = firehose::spawn(client.clone(), config.clone()).await;

    let addr = config
        .listen_address
        .unwrap_or(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000));

    let app = Router::new()
        .route("/", get(index))
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
            db: db.clone(),
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

    #[expect(clippy::print_stdout)]
    if c == 0 {
        let uuid = Uuid::new_v4().to_string();

        _ = sqlx::query!(
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
        .map_err(|e| e.into())
        .and_then(|r| r)
        .context("failed to serve app")
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    // Dispatch out to a separate function without a derive macro to help rust-analyzer along.
    run().await
}
