use anyhow::Result;
use deadpool_diesel::sqlite::{Manager, Pool, Runtime};

#[tracing::instrument(skip_all)]
/// Establish a connection to the database
/// Takes a database URL as an argument (like "sqlite://data/sqlite.db")
pub(crate) fn establish_pool(database_url: &str) -> Result<Pool> {
    tracing::debug!("Establishing database connection");
    let manager = Manager::new(database_url, Runtime::Tokio1);
    let pool = Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("should be able to create connection pool");
    tracing::debug!("Database connection established");
    Ok(pool)
}
