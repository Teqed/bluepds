/// HACK: store private user preferences in the PDS.
///
/// We shouldn't have to know about any bsky endpoints to store private user data.
/// This will _very likely_ be changed in the future.
use atrium_api::app::bsky::actor;
use axum::{Json, routing::post};
use constcat::concat;
use diesel::prelude::*;

use crate::actor_store::ActorStore;

use super::*;

async fn put_preferences(
    user: AuthenticatedUser,
    State(actor_pools): State<std::collections::HashMap<String, ActorPools>>,
    Json(input): Json<actor::put_preferences::Input>,
) -> Result<()> {
    let did = user.did();
    let json_string =
        serde_json::to_string(&input.preferences).context("failed to serialize preferences")?;

    // let conn = &mut actor_pools
    //     .get(&did)
    //     .context("failed to get actor pool")?
    //     .repo
    //     .get()
    //     .await
    //     .expect("failed to get database connection");
    // conn.interact(move |conn| {
    //     diesel::update(accounts::table)
    //         .filter(accounts::did.eq(did))
    //         .set(accounts::private_prefs.eq(json_string))
    //         .execute(conn)
    //         .context("failed to update user preferences")
    // });
    todo!("Use actor_store's preferences writer instead");
    Ok(())
}

async fn get_preferences(
    user: AuthenticatedUser,
    State(actor_pools): State<std::collections::HashMap<String, ActorPools>>,
) -> Result<Json<actor::get_preferences::Output>> {
    let did = user.did();
    // let conn = &mut actor_pools
    //     .get(&did)
    //     .context("failed to get actor pool")?
    //     .repo
    //     .get()
    //     .await
    //     .expect("failed to get database connection");

    // #[derive(QueryableByName)]
    // struct Prefs {
    //     #[diesel(sql_type = diesel::sql_types::Text)]
    //     private_prefs: Option<String>,
    // }

    // let result = conn
    //     .interact(move |conn| {
    //         diesel::sql_query("SELECT private_prefs FROM accounts WHERE did = ?")
    //             .bind::<diesel::sql_types::Text, _>(did)
    //             .get_result::<Prefs>(conn)
    //     })
    //     .await
    //     .expect("failed to fetch preferences");

    // if let Some(prefs_json) = result.private_prefs {
    //     let prefs: actor::defs::Preferences =
    //         serde_json::from_str(&prefs_json).context("failed to deserialize preferences")?;

    //     Ok(Json(
    //         actor::get_preferences::OutputData { preferences: prefs }.into(),
    //     ))
    // } else {
    //     Ok(Json(
    //         actor::get_preferences::OutputData {
    //             preferences: Vec::new(),
    //         }
    //         .into(),
    //     ))
    // }
    todo!("Use actor_store's preferences writer instead");
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
