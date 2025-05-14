use anyhow::Result;
use diesel::prelude::*;
use dotenvy::dotenv;
use rocket_sync_db_pools::database;
use std::env;
use std::fmt::{Debug, Formatter};

#[database("sqlite_db")]
pub struct DbConn(SqliteConnection);

impl Debug for DbConn {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[tracing::instrument(skip_all)]
pub fn establish_connection_for_sequencer() -> Result<SqliteConnection> {
    dotenv().ok();
    tracing::debug!("Establishing database connection for Sequencer");
    let database_url = env::var("BLUEPDS_DB").unwrap_or("sqlite://data/sqlite.db".into());
    let db = SqliteConnection::establish(&database_url).map_err(|error| {
        let context = format!("Error connecting to {database_url:?}");
        anyhow::Error::new(error).context(context)
    })?;
    Ok(db)
}
