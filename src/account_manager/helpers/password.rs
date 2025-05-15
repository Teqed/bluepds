//! Based on https://github.com/blacksky-algorithms/rsky/blob/main/rsky-pds/src/account_manager/helpers/password.rs
//! blacksky-algorithms/rsky is licensed under the Apache License 2.0
//!
//! Modified for SQLite backend
use anyhow::{Result, anyhow, bail};
use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng},
};
// use base64ct::{Base64, Encoding};
use diesel::*;
use rsky_common::{get_random_str, now};
use rsky_lexicon::com::atproto::server::CreateAppPasswordOutput;
#[expect(unused_imports)]
pub(crate) use rsky_pds::account_manager::helpers::password::{
    UpdateUserPasswordOpts, gen_salt_and_hash, hash_app_password, hash_with_salt, verify,
};
use rsky_pds::models;
use rsky_pds::models::AppPassword;

pub async fn verify_account_password(
    did: &str,
    password: &String,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<bool> {
    use rsky_pds::schema::pds::account::dsl as AccountSchema;

    let did = did.to_owned();
    let found = db
        .get()
        .await?
        .interact(move |conn| {
            AccountSchema::account
                .filter(AccountSchema::did.eq(did))
                .select(models::Account::as_select())
                .first(conn)
                .optional()
        })
        .await
        .expect("Failed to get account")?;
    if let Some(found) = found {
        verify(password, &found.password)
    } else {
        Ok(false)
    }
}

pub async fn verify_app_password(
    did: &str,
    password: &str,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<Option<String>> {
    use rsky_pds::schema::pds::app_password::dsl as AppPasswordSchema;

    let did = did.to_owned();
    let password = password.to_owned();
    let password_encrypted = hash_app_password(&did, &password).await?;
    let found = db
        .get()
        .await?
        .interact(move |conn| {
            AppPasswordSchema::app_password
                .filter(AppPasswordSchema::did.eq(did))
                .filter(AppPasswordSchema::password.eq(password_encrypted))
                .select(AppPassword::as_select())
                .first(conn)
                .optional()
        })
        .await
        .expect("Failed to get app password")?;
    if let Some(found) = found {
        Ok(Some(found.name))
    } else {
        Ok(None)
    }
}

/// create an app password with format:
/// 1234-abcd-5678-efgh
pub async fn create_app_password(
    did: String,
    name: String,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<CreateAppPasswordOutput> {
    let str = &get_random_str()[0..16].to_lowercase();
    let chunks = [&str[0..4], &str[4..8], &str[8..12], &str[12..16]];
    let password = chunks.join("-");
    let password_encrypted = hash_app_password(&did, &password).await?;

    use rsky_pds::schema::pds::app_password::dsl as AppPasswordSchema;

    let created_at = now();

    db.get()
        .await?
        .interact(move |conn| {
            let got: Option<AppPassword> = insert_into(AppPasswordSchema::app_password)
                .values((
                    AppPasswordSchema::did.eq(did),
                    AppPasswordSchema::name.eq(&name),
                    AppPasswordSchema::password.eq(password_encrypted),
                    AppPasswordSchema::createdAt.eq(&created_at),
                ))
                .returning(AppPassword::as_select())
                .get_result(conn)
                .optional()?;
            if got.is_some() {
                Ok(CreateAppPasswordOutput {
                    name,
                    password,
                    created_at,
                })
            } else {
                bail!("could not create app-specific password")
            }
        })
        .await
        .expect("Failed to create app password")
}

pub async fn list_app_passwords(
    did: &str,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<Vec<(String, String)>> {
    use rsky_pds::schema::pds::app_password::dsl as AppPasswordSchema;

    let did = did.to_owned();
    db.get()
        .await?
        .interact(move |conn| {
            Ok(AppPasswordSchema::app_password
                .filter(AppPasswordSchema::did.eq(did))
                .select((AppPasswordSchema::name, AppPasswordSchema::createdAt))
                .get_results(conn)?)
        })
        .await
        .expect("Failed to list app passwords")
}

pub async fn update_user_password(
    opts: UpdateUserPasswordOpts,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<()> {
    use rsky_pds::schema::pds::account::dsl as AccountSchema;

    db.get()
        .await?
        .interact(move |conn| {
            update(AccountSchema::account)
                .filter(AccountSchema::did.eq(opts.did))
                .set(AccountSchema::password.eq(opts.password_encrypted))
                .execute(conn)?;
            Ok(())
        })
        .await
        .expect("Failed to update user password")
}

pub async fn delete_app_password(
    did: &str,
    name: &str,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<()> {
    use rsky_pds::schema::pds::app_password::dsl as AppPasswordSchema;

    let did = did.to_owned();
    let name = name.to_owned();
    db.get()
        .await?
        .interact(move |conn| {
            delete(AppPasswordSchema::app_password)
                .filter(AppPasswordSchema::did.eq(did))
                .filter(AppPasswordSchema::name.eq(name))
                .execute(conn)?;
            Ok(())
        })
        .await
        .expect("Failed to delete app password")
}
