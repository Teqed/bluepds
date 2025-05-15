//! Based on https://github.com/blacksky-algorithms/rsky/blob/main/rsky-pds/src/account_manager/helpers/email_token.rs
//! blacksky-algorithms/rsky is licensed under the Apache License 2.0
//!
//! Modified for SQLite backend
#![allow(unnameable_types, unused_qualifications)]
use anyhow::{Result, bail};
use diesel::*;
use rsky_common::time::{MINUTE, from_str_to_utc, less_than_ago_s};
use rsky_pds::apis::com::atproto::server::get_random_token;
use rsky_pds::models::EmailToken;

pub async fn create_email_token(
    did: &str,
    purpose: EmailTokenPurpose,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<String> {
    use rsky_pds::schema::pds::email_token::dsl as EmailTokenSchema;
    let token = get_random_token().to_uppercase();
    let now = rsky_common::now();

    let did = did.to_owned();
    db.get()
        .await?
        .interact(move |conn| {
            _ = insert_into(EmailTokenSchema::email_token)
                .values((
                    EmailTokenSchema::purpose.eq(purpose),
                    EmailTokenSchema::did.eq(did),
                    EmailTokenSchema::token.eq(&token),
                    EmailTokenSchema::requestedAt.eq(&now),
                ))
                .on_conflict((EmailTokenSchema::purpose, EmailTokenSchema::did))
                .do_update()
                .set((
                    EmailTokenSchema::token.eq(&token),
                    EmailTokenSchema::requestedAt.eq(&now),
                ))
                .execute(conn)?;
            Ok(token)
        })
        .await
        .expect("Failed to create email token")
}

pub async fn assert_valid_token(
    did: &str,
    purpose: EmailTokenPurpose,
    token: &str,
    expiration_len: Option<i32>,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<()> {
    let expiration_len = expiration_len.unwrap_or(MINUTE * 15);
    use rsky_pds::schema::pds::email_token::dsl as EmailTokenSchema;

    let did = did.to_owned();
    let token = token.to_owned();
    let res = db
        .get()
        .await?
        .interact(move |conn| {
            EmailTokenSchema::email_token
                .filter(EmailTokenSchema::purpose.eq(purpose))
                .filter(EmailTokenSchema::did.eq(did))
                .filter(EmailTokenSchema::token.eq(token.to_uppercase()))
                .select(EmailToken::as_select())
                .first(conn)
                .optional()
        })
        .await
        .expect("Failed to assert token")?;
    if let Some(res) = res {
        let requested_at = from_str_to_utc(&res.requested_at);
        let expired = !less_than_ago_s(requested_at, expiration_len);
        if expired {
            bail!("Token is expired")
        }
        Ok(())
    } else {
        bail!("Token is invalid")
    }
}

pub async fn assert_valid_token_and_find_did(
    purpose: EmailTokenPurpose,
    token: &str,
    expiration_len: Option<i32>,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<String> {
    let expiration_len = expiration_len.unwrap_or(MINUTE * 15);
    use rsky_pds::schema::pds::email_token::dsl as EmailTokenSchema;

    let token = token.to_owned();
    let res = db
        .get()
        .await?
        .interact(move |conn| {
            EmailTokenSchema::email_token
                .filter(EmailTokenSchema::purpose.eq(purpose))
                .filter(EmailTokenSchema::token.eq(token.to_uppercase()))
                .select(EmailToken::as_select())
                .first(conn)
                .optional()
        })
        .await
        .expect("Failed to assert token")?;
    if let Some(res) = res {
        let requested_at = from_str_to_utc(&res.requested_at);
        let expired = !less_than_ago_s(requested_at, expiration_len);
        if expired {
            bail!("Token is expired")
        }
        Ok(res.did)
    } else {
        bail!("Token is invalid")
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    Default,
    serde::Serialize,
    serde::Deserialize,
    AsExpression,
)]
#[diesel(sql_type = sql_types::Text)]
pub enum EmailTokenPurpose {
    #[default]
    ConfirmEmail,
    UpdateEmail,
    ResetPassword,
    DeleteAccount,
    PlcOperation,
}

impl EmailTokenPurpose {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ConfirmEmail => "confirm_email",
            Self::UpdateEmail => "update_email",
            Self::ResetPassword => "reset_password",
            Self::DeleteAccount => "delete_account",
            Self::PlcOperation => "plc_operation",
        }
    }

    pub fn from_str(s: &str) -> Result<Self> {
        match s {
            "confirm_email" => Ok(Self::ConfirmEmail),
            "update_email" => Ok(Self::UpdateEmail),
            "reset_password" => Ok(Self::ResetPassword),
            "delete_account" => Ok(Self::DeleteAccount),
            "plc_operation" => Ok(Self::PlcOperation),
            _ => bail!("Unable to parse as EmailTokenPurpose: `{s:?}`"),
        }
    }
}

impl<DB> Queryable<sql_types::Text, DB> for EmailTokenPurpose
where
    DB: backend::Backend,
    String: deserialize::FromSql<sql_types::Text, DB>,
{
    type Row = String;

    fn build(s: String) -> deserialize::Result<Self> {
        Ok(Self::from_str(&s)?)
    }
}

impl serialize::ToSql<sql_types::Text, sqlite::Sqlite> for EmailTokenPurpose
where
    String: serialize::ToSql<sql_types::Text, sqlite::Sqlite>,
{
    fn to_sql<'lifetime>(
        &'lifetime self,
        out: &mut serialize::Output<'lifetime, '_, sqlite::Sqlite>,
    ) -> serialize::Result {
        serialize::ToSql::<sql_types::Text, sqlite::Sqlite>::to_sql(
            match self {
                Self::ConfirmEmail => "confirm_email",
                Self::UpdateEmail => "update_email",
                Self::ResetPassword => "reset_password",
                Self::DeleteAccount => "delete_account",
                Self::PlcOperation => "plc_operation",
            },
            out,
        )
    }
}

pub async fn delete_email_token(
    did: &str,
    purpose: EmailTokenPurpose,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<()> {
    use rsky_pds::schema::pds::email_token::dsl as EmailTokenSchema;
    let did = did.to_owned();
    _ = db
        .get()
        .await?
        .interact(move |conn| {
            delete(EmailTokenSchema::email_token)
                .filter(EmailTokenSchema::did.eq(did))
                .filter(EmailTokenSchema::purpose.eq(purpose))
                .execute(conn)
        })
        .await
        .expect("Failed to delete token")?;
    Ok(())
}

pub async fn delete_all_email_tokens(
    did: &str,
    db: &deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
) -> Result<()> {
    use rsky_pds::schema::pds::email_token::dsl as EmailTokenSchema;

    let did = did.to_owned();
    _ = db
        .get()
        .await?
        .interact(move |conn| {
            delete(EmailTokenSchema::email_token)
                .filter(EmailTokenSchema::did.eq(did))
                .execute(conn)
        })
        .await
        .expect("Failed to delete all tokens")?;

    Ok(())
}
