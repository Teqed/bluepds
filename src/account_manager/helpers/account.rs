//! Based on https://github.com/blacksky-algorithms/rsky/blob/main/rsky-pds/src/account_manager/helpers/account.rs
//! blacksky-algorithms/rsky is licensed under the Apache License 2.0
//!
//! Modified for SQLite backend
use crate::schema::pds::account::dsl as AccountSchema;
use crate::schema::pds::account::table as AccountTable;
use crate::schema::pds::actor::dsl as ActorSchema;
use crate::schema::pds::actor::table as ActorTable;
use anyhow::Result;
use chrono::DateTime;
use chrono::offset::Utc as UtcOffset;
use diesel::result::{DatabaseErrorKind, Error as DieselError};
use diesel::*;
use rsky_common::RFC3339_VARIANT;
use rsky_lexicon::com::atproto::admin::StatusAttr;
#[expect(unused_imports)]
pub(crate) use rsky_pds::account_manager::helpers::account::{
    AccountStatus, ActorAccount, AvailabilityFlags, FormattedAccountStatus,
    GetAccountAdminStatusOutput, format_account_status,
};
use std::ops::Add;
use std::time::SystemTime;
use thiserror::Error;

use diesel::dsl::{LeftJoinOn, exists, not};
use diesel::helper_types::Eq;

#[derive(Error, Debug)]
pub enum AccountHelperError {
    #[error("UserAlreadyExistsError")]
    UserAlreadyExistsError,
    #[error("DatabaseError: `{0}`")]
    DieselError(String),
}
pub type ActorJoinAccount =
    LeftJoinOn<ActorTable, AccountTable, Eq<ActorSchema::did, AccountSchema::did>>;
pub type BoxedQuery<'life> = dsl::IntoBoxed<'life, ActorJoinAccount, sqlite::Sqlite>;
pub fn select_account_qb(flags: Option<AvailabilityFlags>) -> BoxedQuery<'static> {
    let AvailabilityFlags {
        include_taken_down,
        include_deactivated,
    } = flags.unwrap_or(AvailabilityFlags {
        include_taken_down: Some(false),
        include_deactivated: Some(false),
    });
    let include_taken_down = include_taken_down.unwrap_or(false);
    let include_deactivated = include_deactivated.unwrap_or(false);

    let mut builder = ActorSchema::actor
        .left_join(AccountSchema::account.on(ActorSchema::did.eq(AccountSchema::did)))
        .into_boxed();
    if !include_taken_down {
        builder = builder.filter(ActorSchema::takedownRef.is_null());
    }
    if !include_deactivated {
        builder = builder.filter(ActorSchema::deactivatedAt.is_null());
    }
    builder
}

pub async fn get_account(
    _handle_or_did: &str,
    flags: Option<AvailabilityFlags>,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<Option<ActorAccount>> {
    let handle_or_did = _handle_or_did.to_owned();
    let found = db
        .get()
        .await?
        .interact(move |conn| {
            let mut builder = select_account_qb(flags);
            if handle_or_did.starts_with("did:") {
                builder = builder.filter(ActorSchema::did.eq(handle_or_did));
            } else {
                builder = builder.filter(ActorSchema::handle.eq(handle_or_did));
            }

            builder
                .select((
                    ActorSchema::did,
                    ActorSchema::handle,
                    ActorSchema::createdAt,
                    ActorSchema::takedownRef,
                    ActorSchema::deactivatedAt,
                    ActorSchema::deleteAfter,
                    AccountSchema::email.nullable(),
                    AccountSchema::emailConfirmedAt.nullable(),
                    AccountSchema::invitesDisabled.nullable(),
                ))
                .first::<(
                    String,
                    Option<String>,
                    String,
                    Option<String>,
                    Option<String>,
                    Option<String>,
                    Option<String>,
                    Option<String>,
                    Option<i16>,
                )>(conn)
                .map(|res| ActorAccount {
                    did: res.0,
                    handle: res.1,
                    created_at: res.2,
                    takedown_ref: res.3,
                    deactivated_at: res.4,
                    delete_after: res.5,
                    email: res.6,
                    email_confirmed_at: res.7,
                    invites_disabled: res.8,
                })
                .optional()
        })
        .await
        .expect("Failed to get account")?;
    Ok(found)
}

pub async fn get_account_by_email(
    _email: &str,
    flags: Option<AvailabilityFlags>,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<Option<ActorAccount>> {
    let email = _email.to_owned();
    let found = db
        .get()
        .await?
        .interact(move |conn| {
            select_account_qb(flags)
                .select((
                    ActorSchema::did,
                    ActorSchema::handle,
                    ActorSchema::createdAt,
                    ActorSchema::takedownRef,
                    ActorSchema::deactivatedAt,
                    ActorSchema::deleteAfter,
                    AccountSchema::email.nullable(),
                    AccountSchema::emailConfirmedAt.nullable(),
                    AccountSchema::invitesDisabled.nullable(),
                ))
                .filter(AccountSchema::email.eq(email.to_lowercase()))
                .first::<(
                    String,
                    Option<String>,
                    String,
                    Option<String>,
                    Option<String>,
                    Option<String>,
                    Option<String>,
                    Option<String>,
                    Option<i16>,
                )>(conn)
                .map(|res| ActorAccount {
                    did: res.0,
                    handle: res.1,
                    created_at: res.2,
                    takedown_ref: res.3,
                    deactivated_at: res.4,
                    delete_after: res.5,
                    email: res.6,
                    email_confirmed_at: res.7,
                    invites_disabled: res.8,
                })
                .optional()
        })
        .await
        .expect("Failed to get account")?;
    Ok(found)
}

pub async fn register_actor(
    did: String,
    handle: String,
    deactivated: Option<bool>,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<()> {
    let system_time = SystemTime::now();
    let dt: DateTime<UtcOffset> = system_time.into();
    let created_at = format!("{}", dt.format(RFC3339_VARIANT));
    let deactivate_at = match deactivated {
        Some(true) => Some(created_at.clone()),
        _ => None,
    };
    let deactivate_after = match deactivated {
        Some(true) => {
            let exp = dt.add(chrono::Duration::days(3));
            Some(format!("{}", exp.format(RFC3339_VARIANT)))
        }
        _ => None,
    };

    let _: String = db
        .get()
        .await?
        .interact(move |conn| {
            insert_into(ActorSchema::actor)
                .values((
                    ActorSchema::did.eq(did),
                    ActorSchema::handle.eq(handle),
                    ActorSchema::createdAt.eq(created_at),
                    ActorSchema::deactivatedAt.eq(deactivate_at),
                    ActorSchema::deleteAfter.eq(deactivate_after),
                ))
                .on_conflict_do_nothing()
                .returning(ActorSchema::did)
                .get_result(conn)
        })
        .await
        .expect("Failed to register actor")?;
    Ok(())
}

pub async fn register_account(
    did: String,
    email: String,
    password: String,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<()> {
    let created_at = rsky_common::now();

    // @TODO record recovery key for bring your own recovery key
    let _: String = db
        .get()
        .await?
        .interact(move |conn| {
            insert_into(AccountSchema::account)
                .values((
                    AccountSchema::did.eq(did),
                    AccountSchema::email.eq(email),
                    AccountSchema::password.eq(password),
                    AccountSchema::createdAt.eq(created_at),
                ))
                .on_conflict_do_nothing()
                .returning(AccountSchema::did)
                .get_result(conn)
        })
        .await
        .expect("Failed to register account")?;
    Ok(())
}

pub async fn delete_account(
    did: &str,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
    actor_db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<()> {
    use crate::schema::actor_store::repo_root::dsl as RepoRootSchema;
    use crate::schema::pds::email_token::dsl as EmailTokenSchema;
    use crate::schema::pds::refresh_token::dsl as RefreshTokenSchema;

    let did_clone = did.to_owned();
    _ = actor_db
        .get()
        .await?
        .interact(move |conn| {
            delete(RepoRootSchema::repo_root)
                .filter(RepoRootSchema::did.eq(&did_clone))
                .execute(conn)
        })
        .await
        .expect("Failed to delete actor")?;
    let did_clone = did.to_owned();
    _ = db
        .get()
        .await?
        .interact(move |conn| {
            _ = delete(EmailTokenSchema::email_token)
                .filter(EmailTokenSchema::did.eq(&did_clone))
                .execute(conn)?;
            _ = delete(RefreshTokenSchema::refresh_token)
                .filter(RefreshTokenSchema::did.eq(&did_clone))
                .execute(conn)?;
            _ = delete(AccountSchema::account)
                .filter(AccountSchema::did.eq(&did_clone))
                .execute(conn)?;
            delete(ActorSchema::actor)
                .filter(ActorSchema::did.eq(&did_clone))
                .execute(conn)
        })
        .await
        .expect("Failed to delete account")?;

    let data_repo_file = format!("data/repo/{}.db", did.to_owned());
    let data_blob_path = format!("data/blob/{}", did);
    let data_blob_path = std::path::Path::new(&data_blob_path);
    let data_repo_file = std::path::Path::new(&data_repo_file);
    if data_repo_file.exists() {
        std::fs::remove_file(data_repo_file)?;
    };
    if data_blob_path.exists() {
        std::fs::remove_dir_all(data_blob_path)?;
    };
    Ok(())
}

pub async fn update_account_takedown_status(
    did: &str,
    takedown: StatusAttr,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<()> {
    let takedown_ref: Option<String> = match takedown.applied {
        true => takedown
            .r#ref
            .map_or_else(|| Some(rsky_common::now()), Some),
        false => None,
    };
    let did = did.to_owned();
    _ = db
        .get()
        .await?
        .interact(move |conn| {
            update(ActorSchema::actor)
                .filter(ActorSchema::did.eq(did))
                .set((ActorSchema::takedownRef.eq(takedown_ref),))
                .execute(conn)
        })
        .await
        .expect("Failed to update account takedown status")?;
    Ok(())
}

pub async fn deactivate_account(
    did: &str,
    delete_after: Option<String>,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<()> {
    let did = did.to_owned();
    _ = db
        .get()
        .await?
        .interact(move |conn| {
            update(ActorSchema::actor)
                .filter(ActorSchema::did.eq(did))
                .set((
                    ActorSchema::deactivatedAt.eq(rsky_common::now()),
                    ActorSchema::deleteAfter.eq(delete_after),
                ))
                .execute(conn)
        })
        .await
        .expect("Failed to deactivate account")?;
    Ok(())
}

pub async fn activate_account(
    did: &str,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<()> {
    let did = did.to_owned();
    _ = db
        .get()
        .await?
        .interact(move |conn| {
            update(ActorSchema::actor)
                .filter(ActorSchema::did.eq(did))
                .set((
                    ActorSchema::deactivatedAt.eq::<Option<String>>(None),
                    ActorSchema::deleteAfter.eq::<Option<String>>(None),
                ))
                .execute(conn)
        })
        .await
        .expect("Failed to activate account")?;
    Ok(())
}

pub async fn update_email(
    did: &str,
    email: &str,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<()> {
    let did = did.to_owned();
    let email = email.to_owned();
    let res = db
        .get()
        .await?
        .interact(move |conn| {
            update(AccountSchema::account)
                .filter(AccountSchema::did.eq(did))
                .set((
                    AccountSchema::email.eq(email.to_lowercase()),
                    AccountSchema::emailConfirmedAt.eq::<Option<String>>(None),
                ))
                .execute(conn)
        })
        .await
        .expect("Failed to update email");

    match res {
        Ok(_) => Ok(()),
        Err(DieselError::DatabaseError(kind, _)) => match kind {
            DatabaseErrorKind::UniqueViolation => Err(anyhow::Error::new(
                AccountHelperError::UserAlreadyExistsError,
            )),
            _ => Err(anyhow::Error::new(AccountHelperError::DieselError(
                format!("{:?}", kind),
            ))),
        },
        Err(e) => Err(anyhow::Error::new(e)),
    }
}

pub async fn update_handle(
    did: &str,
    handle: &str,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<()> {
    use crate::schema::pds::actor;

    let actor2 = diesel::alias!(actor as actor2);

    let did = did.to_owned();
    let handle = handle.to_owned();
    let res = db
        .get()
        .await?
        .interact(move |conn| {
            update(ActorSchema::actor)
                .filter(ActorSchema::did.eq(did))
                .filter(not(exists(actor2.filter(ActorSchema::handle.eq(&handle)))))
                .set((ActorSchema::handle.eq(&handle),))
                .execute(conn)
        })
        .await
        .expect("Failed to update handle")?;

    if res < 1 {
        return Err(anyhow::Error::new(
            AccountHelperError::UserAlreadyExistsError,
        ));
    }
    Ok(())
}

pub async fn set_email_confirmed_at(
    did: &str,
    email_confirmed_at: String,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<()> {
    let did = did.to_owned();
    _ = db
        .get()
        .await?
        .interact(move |conn| {
            update(AccountSchema::account)
                .filter(AccountSchema::did.eq(did))
                .set(AccountSchema::emailConfirmedAt.eq(email_confirmed_at))
                .execute(conn)
        })
        .await
        .expect("Failed to set email confirmed at")?;
    Ok(())
}

pub async fn get_account_admin_status(
    did: &str,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<Option<GetAccountAdminStatusOutput>> {
    let did = did.to_owned();
    let res: Option<(Option<String>, Option<String>)> = db
        .get()
        .await?
        .interact(move |conn| {
            ActorSchema::actor
                .filter(ActorSchema::did.eq(did))
                .select((ActorSchema::takedownRef, ActorSchema::deactivatedAt))
                .first(conn)
                .optional()
        })
        .await
        .expect("Failed to get account admin status")?;
    match res {
        None => Ok(None),
        Some(res) => {
            let takedown = res.0.map_or(
                StatusAttr {
                    applied: false,
                    r#ref: None,
                },
                |takedown_ref| StatusAttr {
                    applied: true,
                    r#ref: Some(takedown_ref),
                },
            );
            let deactivated = match res.1 {
                Some(_) => StatusAttr {
                    applied: true,
                    r#ref: None,
                },
                None => StatusAttr {
                    applied: false,
                    r#ref: None,
                },
            };
            Ok(Some(GetAccountAdminStatusOutput {
                takedown,
                deactivated,
            }))
        }
    }
}
