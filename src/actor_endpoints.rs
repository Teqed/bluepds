use atrium_api::app::bsky::actor;
use axum::{Json, routing::post};
use constcat::concat;
use diesel::prelude::*;

use super::*;

async fn put_preferences(
    user: AuthenticatedUser,
    State(db): State<Db>,
    Json(input): Json<actor::put_preferences::Input>,
) -> Result<()> {
    let did = user.did();
    let json_string =
        serde_json::to_string(&input.preferences).context("failed to serialize preferences")?;

    // Use the db connection pool to execute the update
    let conn = &mut db.get().context("failed to get database connection")?;
    diesel::sql_query("UPDATE accounts SET private_prefs = ? WHERE did = ?")
        .bind::<diesel::sql_types::Text, _>(json_string)
        .bind::<diesel::sql_types::Text, _>(did)
        .execute(conn)
        .context("failed to update user preferences")?;

    Ok(())
}

async fn get_preferences(
    user: AuthenticatedUser,
    State(db): State<Db>,
) -> Result<Json<actor::get_preferences::Output>> {
    let did = user.did();
    let conn = &mut db.get().context("failed to get database connection")?;

    #[derive(QueryableByName)]
    struct Prefs {
        #[diesel(sql_type = diesel::sql_types::Text)]
        private_prefs: Option<String>,
    }

    let result = diesel::sql_query("SELECT private_prefs FROM accounts WHERE did = ?")
        .bind::<diesel::sql_types::Text, _>(did)
        .get_result::<Prefs>(conn)
        .context("failed to fetch preferences")?;

    if let Some(prefs_json) = result.private_prefs {
        let prefs: actor::defs::Preferences =
            serde_json::from_str(&prefs_json).context("failed to deserialize preferences")?;

        Ok(Json(
            actor::get_preferences::OutputData { preferences: prefs }.into(),
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

/// Register all actor endpoints.
pub(crate) fn routes() -> Router<AppState> {
    // AP /xrpc/app.bsky.actor.putPreferences
    // AG /xrpc/app.bsky.actor.getPreferences
    Router::new()
        .route(
            concat!("/", actor::put_preferences::NSID),
            post(put_preferences),
        )
        .route(
            concat!("/", actor::get_preferences::NSID),
            get(get_preferences),
        )
}
