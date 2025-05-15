//! Based on https://github.com/blacksky-algorithms/rsky/blob/main/rsky-pds/src/account_manager/helpers/auth.rs
//! blacksky-algorithms/rsky is licensed under the Apache License 2.0
//!
//! Modified for SQLite backend
use anyhow::Result;
use diesel::*;
use rsky_common::time::from_micros_to_utc;
use rsky_common::{RFC3339_VARIANT, get_random_str};
#[expect(unused_imports)]
pub(crate) use rsky_pds::account_manager::helpers::auth::{
    AuthHelperError, AuthToken, CreateTokensOpts, CustomClaimObj, RefreshGracePeriodOpts,
    RefreshToken, ServiceJwtHeader, ServiceJwtParams, ServiceJwtPayload, create_access_token,
    create_refresh_token, create_service_jwt, create_tokens, decode_refresh_token,
};
use rsky_pds::models;

pub async fn store_refresh_token(
    payload: RefreshToken,
    app_password_name: Option<String>,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<()> {
    use crate::schema::pds::refresh_token::dsl as RefreshTokenSchema;

    let exp = from_micros_to_utc((payload.exp.as_millis() / 1000) as i64);

    _ = db
        .get()
        .await?
        .interact(move |conn| {
            insert_into(RefreshTokenSchema::refresh_token)
                .values((
                    RefreshTokenSchema::id.eq(payload.jti),
                    RefreshTokenSchema::did.eq(payload.sub),
                    RefreshTokenSchema::appPasswordName.eq(app_password_name),
                    RefreshTokenSchema::expiresAt.eq(format!("{}", exp.format(RFC3339_VARIANT))),
                ))
                .on_conflict_do_nothing() // E.g. when re-granting during a refresh grace period
                .execute(conn)
        })
        .await
        .expect("Failed to store refresh token")?;

    Ok(())
}

pub async fn revoke_refresh_token(
    id: String,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<bool> {
    use crate::schema::pds::refresh_token::dsl as RefreshTokenSchema;
    db.get()
        .await?
        .interact(move |conn| {
            let deleted_rows = delete(RefreshTokenSchema::refresh_token)
                .filter(RefreshTokenSchema::id.eq(id))
                .get_results::<models::RefreshToken>(conn)?;

            Ok(!deleted_rows.is_empty())
        })
        .await
        .expect("Failed to revoke refresh token")
}

pub async fn revoke_refresh_tokens_by_did(
    did: &str,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<bool> {
    use crate::schema::pds::refresh_token::dsl as RefreshTokenSchema;
    let did = did.to_owned();
    db.get()
        .await?
        .interact(move |conn| {
            let deleted_rows = delete(RefreshTokenSchema::refresh_token)
                .filter(RefreshTokenSchema::did.eq(did))
                .get_results::<models::RefreshToken>(conn)?;

            Ok(!deleted_rows.is_empty())
        })
        .await
        .expect("Failed to revoke refresh tokens by DID")
}

pub async fn revoke_app_password_refresh_token(
    did: &str,
    app_pass_name: &str,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<bool> {
    use crate::schema::pds::refresh_token::dsl as RefreshTokenSchema;

    let did = did.to_owned();
    let app_pass_name = app_pass_name.to_owned();
    db.get()
        .await?
        .interact(move |conn| {
            let deleted_rows = delete(RefreshTokenSchema::refresh_token)
                .filter(RefreshTokenSchema::did.eq(did))
                .filter(RefreshTokenSchema::appPasswordName.eq(app_pass_name))
                .get_results::<models::RefreshToken>(conn)?;

            Ok(!deleted_rows.is_empty())
        })
        .await
        .expect("Failed to revoke app password refresh token")
}

pub async fn get_refresh_token(
    id: &str,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<Option<models::RefreshToken>> {
    use crate::schema::pds::refresh_token::dsl as RefreshTokenSchema;
    let id = id.to_owned();
    db.get()
        .await?
        .interact(move |conn| {
            Ok(RefreshTokenSchema::refresh_token
                .find(id)
                .first(conn)
                .optional()?)
        })
        .await
        .expect("Failed to get refresh token")
}

pub async fn delete_expired_refresh_tokens(
    did: &str,
    now: String,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<()> {
    use crate::schema::pds::refresh_token::dsl as RefreshTokenSchema;
    let did = did.to_owned();

    db.get()
        .await?
        .interact(move |conn| {
            _ = delete(RefreshTokenSchema::refresh_token)
                .filter(RefreshTokenSchema::did.eq(did))
                .filter(RefreshTokenSchema::expiresAt.le(now))
                .execute(conn)?;
            Ok(())
        })
        .await
        .expect("Failed to delete expired refresh tokens")
}

pub async fn add_refresh_grace_period(
    opts: RefreshGracePeriodOpts,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<()> {
    db.get()
        .await?
        .interact(move |conn| {
            let RefreshGracePeriodOpts {
                id,
                expires_at,
                next_id,
            } = opts;
            use crate::schema::pds::refresh_token::dsl as RefreshTokenSchema;

            drop(
                update(RefreshTokenSchema::refresh_token)
                    .filter(RefreshTokenSchema::id.eq(id))
                    .filter(
                        RefreshTokenSchema::nextId
                            .is_null()
                            .or(RefreshTokenSchema::nextId.eq(&next_id)),
                    )
                    .set((
                        RefreshTokenSchema::expiresAt.eq(expires_at),
                        RefreshTokenSchema::nextId.eq(&next_id),
                    ))
                    .returning(models::RefreshToken::as_select())
                    .get_results(conn)
                    .map_err(|error| {
                        anyhow::Error::new(AuthHelperError::ConcurrentRefresh).context(error)
                    })?,
            );
            Ok(())
        })
        .await
        .expect("Failed to add refresh grace period")
}

pub fn get_refresh_token_id() -> String {
    get_random_str()
}
