use super::account_manager::{AccountManager, SharedAccountManager};
use super::config::AppConfig;
use super::db::establish_pool;
pub use super::error::Error;
use super::service_proxy::service_proxy;
use anyhow::Context as _;
use atrium_api::types::string::Did;
use atrium_crypto::keypair::{Export as _, Secp256k1Keypair};
use axum::{Router, extract::FromRef, routing::get};
use clap::Parser;
use clap_verbosity_flag::{InfoLevel, Verbosity, log::LevelFilter};
use deadpool_diesel::sqlite::Pool;
use diesel::prelude::*;
use diesel_migrations::{EmbeddedMigrations, embed_migrations};
use figment::{Figment, providers::Format as _};
use http_cache_reqwest::{CacheMode, HttpCacheOptions, MokaManager};
use rsky_pds::{crawlers::Crawlers, sequencer::Sequencer};
use serde::{Deserialize, Serialize};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    str::FromStr as _,
    sync::Arc,
};
use tokio::{net::TcpListener, sync::RwLock};
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

/// The Shared Sequencer which requests crawls from upstream relays and emits events to the firehose.
pub struct SharedSequencer {
    /// The sequencer instance.
    pub sequencer: RwLock<Sequencer>,
}

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
pub struct Args {
    /// Path to the configuration file
    #[arg(short, long, default_value = "default.toml")]
    pub config: PathBuf,
    /// The verbosity level.
    #[command(flatten)]
    pub verbosity: Verbosity<InfoLevel>,
}

/// The actor pools for the database connections.
pub struct ActorStorage {
    /// The database connection pool for the actor's repository.
    pub repo: Pool,
    /// The file storage path for the actor's blobs.
    pub blob: PathBuf,
}

impl Clone for ActorStorage {
    fn clone(&self) -> Self {
        Self {
            repo: self.repo.clone(),
            blob: self.blob.clone(),
        }
    }
}

#[expect(clippy::arbitrary_source_item_ordering, reason = "arbitrary")]
#[derive(Clone, FromRef)]
/// The application state, shared across all routes.
pub struct AppState {
    /// The application configuration.
    pub(crate) config: AppConfig,
    /// The main database connection pool. Used for common PDS data, like invite codes.
    pub db: Pool,
    /// Actor-specific database connection pools. Hashed by DID.
    pub db_actors: std::collections::HashMap<String, ActorStorage>,

    /// The HTTP client with middleware.
    pub client: Client,
    /// The simple HTTP client.
    pub simple_client: reqwest::Client,
    /// The firehose producer.
    pub sequencer: Arc<SharedSequencer>,
    /// The account manager.
    pub account_manager: Arc<SharedAccountManager>,

    /// The signing key.
    pub signing_key: SigningKey,
    /// The rotation key.
    pub rotation_key: RotationKey,
}

/// The main application entry point.
#[expect(
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    unused_qualifications,
    reason = "main function has high complexity"
)]
pub async fn run() -> anyhow::Result<()> {
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
    super::metrics::setup(config.metrics.as_ref()).context("failed to set up metrics exporter")?;

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

    // Create a database connection manager and pool for the main database.
    let pool =
        establish_pool(&config.db).context("failed to establish database connection pool")?;

    // Create a dictionary of database connection pools for each actor.
    let mut actor_pools = std::collections::HashMap::new();
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
            let actor_repo_pool = establish_pool(&format!("sqlite://{}", path.display()))
                .context("failed to create database connection pool")?;

            let did = Did::from_str(&format!(
                "did:plc:{}",
                path.file_stem()
                    .and_then(|s| s.to_str())
                    .context("failed to get actor DID")?
            ))
            .expect("should be able to parse actor DID")
            .to_string();
            let blob_path = config.blob.path.to_path_buf();
            let actor_storage = ActorStorage {
                repo: actor_repo_pool,
                blob: blob_path.clone(),
            };
            drop(actor_pools.insert(did, actor_storage));
        }
    }
    // Apply pending migrations
    // let conn = pool.get().await?;
    // conn.run_pending_migrations(MIGRATIONS)
    //     .expect("should be able to run migrations");

    let hostname = config.host_name.clone();
    let crawlers: Vec<String> = config
        .firehose
        .relays
        .iter()
        .map(|s| s.to_string())
        .collect();
    let sequencer = Arc::new(SharedSequencer {
        sequencer: RwLock::new(Sequencer::new(
            Crawlers::new(hostname, crawlers.clone()),
            None,
        )),
    });
    let account_manager = SharedAccountManager {
        account_manager: RwLock::new(AccountManager::new(pool.clone())),
    };

    let addr = config
        .listen_address
        .unwrap_or(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000));

    let app = Router::new()
        .route("/", get(super::index))
        .merge(super::oauth::routes())
        .nest(
            "/xrpc",
            super::apis::routes()
                .merge(super::actor_endpoints::routes())
                .fallback(service_proxy),
        )
        // .layer(RateLimitLayer::new(30, Duration::from_secs(30)))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(AppState {
            config: config.clone(),
            db: pool.clone(),
            db_actors: actor_pools.clone(),
            client: client.clone(),
            simple_client,
            sequencer: sequencer.clone(),
            account_manager: Arc::new(account_manager),
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

    let result = conn.interact(move |conn| {
        diesel::sql_query(
            "SELECT (SELECT COUNT(*) FROM account) + (SELECT COUNT(*) FROM invite_code) AS total_count",
        )
        .get_result::<TotalCount>(conn)
    })
    .await
    .expect("should be able to query database")?;

    let c = result.total_count;

    #[expect(clippy::print_stdout)]
    if c == 0 {
        let uuid = Uuid::new_v4().to_string();

        use crate::models::pds as models;
        use crate::schema::pds::invite_code::dsl as InviteCode;
        let uuid_clone = uuid.clone();
        drop(
            conn.interact(move |conn| {
                diesel::insert_into(InviteCode::invite_code)
                    .values(models::InviteCode {
                        code: uuid_clone,
                        available_uses: 1,
                        disabled: 0,
                        for_account: "None".to_owned(),
                        created_by: "None".to_owned(),
                        created_at: "None".to_owned(),
                    })
                    .execute(conn)
                    .context("failed to create new invite code")
            })
            .await
            .expect("should be able to create invite code"),
        );

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
    if cfg!(debug_assertions) {
        info!("debug mode: not requesting crawl");
    } else {
        info!("requesting crawl from upstream relays");
        let mut background_sequencer = sequencer.sequencer.write().await.clone();
        drop(tokio::spawn(
            async move { background_sequencer.start().await },
        ));
    }

    serve
        .await
        .map_err(Into::into)
        .and_then(|r| r)
        .context("failed to serve app")
}
