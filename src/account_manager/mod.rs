//! Based on https://github.com/blacksky-algorithms/rsky/blob/main/rsky-pds/src/account_manager/mod.rs
//! blacksky-algorithms/rsky is licensed under the Apache License 2.0
//!
//! Modified for SQLite backend
use crate::account_manager::helpers::account::{
    AccountStatus, ActorAccount, AvailabilityFlags, GetAccountAdminStatusOutput,
};
use crate::account_manager::helpers::auth::{
    AuthHelperError, CreateTokensOpts, RefreshGracePeriodOpts,
};
use crate::account_manager::helpers::invite::CodeDetail;
use crate::account_manager::helpers::password::UpdateUserPasswordOpts;
use crate::models::pds::EmailTokenPurpose;
use anyhow::Result;
use chrono::DateTime;
use chrono::offset::Utc as UtcOffset;
use cidv10::Cid;
use diesel::*;
use futures::try_join;
use helpers::{account, auth, email_token, invite, password, repo};
use rsky_common::RFC3339_VARIANT;
use rsky_common::time::{HOUR, from_micros_to_str, from_str_to_micros};
use rsky_lexicon::com::atproto::admin::StatusAttr;
use rsky_lexicon::com::atproto::server::{AccountCodes, CreateAppPasswordOutput};
use rsky_pds::account_manager::{
    ConfirmEmailOpts, CreateAccountOpts, DisableInviteCodesOpts, ResetPasswordOpts,
    UpdateAccountPasswordOpts, UpdateEmailOpts,
};
use rsky_pds::auth_verifier::AuthScope;
use secp256k1::{Keypair, Secp256k1, SecretKey};
use std::collections::BTreeMap;
use std::env;
use std::time::SystemTime;
use tokio::sync::RwLock;

pub(crate) mod helpers {
    pub mod account;
    pub mod auth;
    pub mod email_token;
    pub mod invite;
    pub mod password;
    pub mod repo;
}

#[derive(Clone)]
pub struct AccountManager {
    pub db: deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
}
impl std::fmt::Debug for AccountManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccountManager").finish()
    }
}

pub type AccountManagerCreator = Box<
    dyn Fn(
            deadpool_diesel::Pool<
                deadpool_diesel::Manager<SqliteConnection>,
                deadpool_diesel::sqlite::Object,
            >,
        ) -> AccountManager
        + Send
        + Sync,
>;

impl AccountManager {
    pub const fn new(
        db: deadpool_diesel::Pool<
            deadpool_diesel::Manager<SqliteConnection>,
            deadpool_diesel::sqlite::Object,
        >,
    ) -> Self {
        Self { db }
    }

    pub fn creator() -> AccountManagerCreator {
        Box::new(
            move |db: deadpool_diesel::Pool<
                deadpool_diesel::Manager<SqliteConnection>,
                deadpool_diesel::sqlite::Object,
            >|
                  -> Self { Self::new(db) },
        )
    }

    pub async fn get_account(
        &self,
        handle_or_did: &str,
        flags: Option<AvailabilityFlags>,
    ) -> Result<Option<ActorAccount>> {
        account::get_account(handle_or_did, flags, &self.db).await
    }

    pub async fn get_account_by_email(
        &self,
        email: &str,
        flags: Option<AvailabilityFlags>,
    ) -> Result<Option<ActorAccount>> {
        account::get_account_by_email(email, flags, &self.db).await
    }

    pub async fn is_account_activated(&self, did: &str) -> Result<bool> {
        let account = self
            .get_account(
                did,
                Some(AvailabilityFlags {
                    include_taken_down: None,
                    include_deactivated: Some(true),
                }),
            )
            .await?;
        if let Some(account) = account {
            Ok(account.deactivated_at.is_none())
        } else {
            Ok(false)
        }
    }

    pub async fn get_did_for_actor(
        &self,
        handle_or_did: &str,
        flags: Option<AvailabilityFlags>,
    ) -> Result<Option<String>> {
        match self.get_account(handle_or_did, flags).await {
            Ok(Some(got)) => Ok(Some(got.did)),
            _ => Ok(None),
        }
    }

    pub async fn create_account(&self, opts: CreateAccountOpts) -> Result<(String, String)> {
        let CreateAccountOpts {
            did,
            handle,
            email,
            password,
            repo_cid,
            repo_rev,
            invite_code,
            deactivated,
        } = opts;
        let password_encrypted: Option<String> = match password {
            Some(password) => Some(password::gen_salt_and_hash(password)?),
            None => None,
        };
        // Should be a global var so this only happens once
        let secp = Secp256k1::new();
        let private_key = env::var("PDS_JWT_KEY_K256_PRIVATE_KEY_HEX")?;
        let secret_key =
            SecretKey::from_slice(&Result::unwrap(hex::decode(private_key.as_bytes())))?;
        let jwt_key = Keypair::from_secret_key(&secp, &secret_key);
        let (access_jwt, refresh_jwt) = auth::create_tokens(CreateTokensOpts {
            did: did.clone(),
            jwt_key,
            service_did: env::var("PDS_SERVICE_DID").expect("PDS_SERVICE_DID not set"),
            scope: Some(AuthScope::Access),
            jti: None,
            expires_in: None,
        })?;
        let refresh_payload = auth::decode_refresh_token(refresh_jwt.clone(), jwt_key)?;
        let now = rsky_common::now();

        if let Some(invite_code) = invite_code.clone() {
            invite::ensure_invite_is_available(invite_code, &self.db).await?;
        }
        account::register_actor(did.clone(), handle, deactivated, &self.db).await?;
        if let (Some(email), Some(password_encrypted)) = (email, password_encrypted) {
            account::register_account(did.clone(), email, password_encrypted, &self.db).await?;
        }
        invite::record_invite_use(did.clone(), invite_code, now, &self.db).await?;
        auth::store_refresh_token(refresh_payload, None, &self.db).await?;
        repo::update_root(did, repo_cid, repo_rev, &self.db).await?;
        Ok((access_jwt, refresh_jwt))
    }

    pub async fn get_account_admin_status(
        &self,
        did: &str,
    ) -> Result<Option<GetAccountAdminStatusOutput>> {
        account::get_account_admin_status(did, &self.db).await
    }

    pub async fn update_repo_root(&self, did: String, cid: Cid, rev: String) -> Result<()> {
        repo::update_root(did, cid, rev, &self.db).await
    }

    pub async fn delete_account(&self, did: &str) -> Result<()> {
        account::delete_account(did, &self.db).await
    }

    pub async fn takedown_account(&self, did: &str, takedown: StatusAttr) -> Result<()> {
        (_, _) = try_join!(
            account::update_account_takedown_status(did, takedown, &self.db),
            auth::revoke_refresh_tokens_by_did(did, &self.db)
        )?;
        Ok(())
    }

    // @NOTE should always be paired with a sequenceHandle().
    pub async fn update_handle(&self, did: &str, handle: &str) -> Result<()> {
        account::update_handle(did, handle, &self.db).await
    }

    pub async fn deactivate_account(&self, did: &str, delete_after: Option<String>) -> Result<()> {
        account::deactivate_account(did, delete_after, &self.db).await
    }

    pub async fn activate_account(&self, did: &str) -> Result<()> {
        account::activate_account(did, &self.db).await
    }

    pub async fn get_account_status(&self, handle_or_did: &str) -> Result<AccountStatus> {
        let got = account::get_account(
            handle_or_did,
            Some(AvailabilityFlags {
                include_deactivated: Some(true),
                include_taken_down: Some(true),
            }),
            &self.db,
        )
        .await?;
        let res = account::format_account_status(got);
        match res.active {
            true => Ok(AccountStatus::Active),
            false => Ok(res.status.expect("Account status not properly formatted.")),
        }
    }

    // Auth
    // ----------
    pub async fn create_session(
        &self,
        did: String,
        app_password_name: Option<String>,
    ) -> Result<(String, String)> {
        let secp = Secp256k1::new();
        let private_key = env::var("PDS_JWT_KEY_K256_PRIVATE_KEY_HEX")?;
        let secret_key = SecretKey::from_slice(&hex::decode(private_key.as_bytes())?)?;
        let jwt_key = Keypair::from_secret_key(&secp, &secret_key);
        let scope = if app_password_name.is_none() {
            AuthScope::Access
        } else {
            AuthScope::AppPass
        };
        let (access_jwt, refresh_jwt) = auth::create_tokens(CreateTokensOpts {
            did,
            jwt_key,
            service_did: env::var("PDS_SERVICE_DID").expect("PDS_SERVICE_DID not set"),
            scope: Some(scope),
            jti: None,
            expires_in: None,
        })?;
        let refresh_payload = auth::decode_refresh_token(refresh_jwt.clone(), jwt_key)?;
        auth::store_refresh_token(refresh_payload, app_password_name, &self.db).await?;
        Ok((access_jwt, refresh_jwt))
    }

    pub async fn rotate_refresh_token(&self, id: &String) -> Result<Option<(String, String)>> {
        let token = auth::get_refresh_token(id, &self.db).await?;
        if let Some(token) = token {
            let system_time = SystemTime::now();
            let dt: DateTime<UtcOffset> = system_time.into();
            let now = format!("{}", dt.format(RFC3339_VARIANT));

            // take the chance to tidy all of a user's expired tokens
            // does not need to be transactional since this is just best-effort
            auth::delete_expired_refresh_tokens(&token.did, now, &self.db).await?;

            // Shorten the refresh token lifespan down from its
            // original expiration time to its revocation grace period.
            let prev_expires_at = from_str_to_micros(&token.expires_at);

            const REFRESH_GRACE_MS: i32 = 2 * HOUR;
            let grace_expires_at = dt.timestamp_micros() + REFRESH_GRACE_MS as i64;

            let expires_at = if grace_expires_at < prev_expires_at {
                grace_expires_at
            } else {
                prev_expires_at
            };

            if expires_at <= dt.timestamp_micros() {
                return Ok(None);
            }

            // Determine the next refresh token id: upon refresh token
            // reuse you always receive a refresh token with the same id.
            let next_id = token.next_id.unwrap_or_else(auth::get_refresh_token_id);

            let secp = Secp256k1::new();
            let private_key = env::var("PDS_JWT_KEY_K256_PRIVATE_KEY_HEX")
                .expect("PDS_JWT_KEY_K256_PRIVATE_KEY_HEX not set");
            let secret_key =
                SecretKey::from_slice(&hex::decode(private_key.as_bytes()).expect("Invalid key"))?;
            let jwt_key = Keypair::from_secret_key(&secp, &secret_key);

            let (access_jwt, refresh_jwt) = auth::create_tokens(CreateTokensOpts {
                did: token.did,
                jwt_key,
                service_did: env::var("PDS_SERVICE_DID").expect("PDS_SERVICE_DID not set"),
                scope: Some(if token.app_password_name.is_none() {
                    AuthScope::Access
                } else {
                    AuthScope::AppPass
                }),
                jti: Some(next_id.clone()),
                expires_in: None,
            })?;
            let refresh_payload = auth::decode_refresh_token(refresh_jwt.clone(), jwt_key)?;
            match try_join!(
                auth::add_refresh_grace_period(
                    RefreshGracePeriodOpts {
                        id: id.clone(),
                        expires_at: from_micros_to_str(expires_at),
                        next_id
                    },
                    &self.db
                ),
                auth::store_refresh_token(refresh_payload, token.app_password_name, &self.db)
            ) {
                Ok(_) => Ok(Some((access_jwt, refresh_jwt))),
                Err(e) => match e.downcast_ref() {
                    Some(AuthHelperError::ConcurrentRefresh) => {
                        Box::pin(self.rotate_refresh_token(id)).await
                    }
                    _ => Err(e),
                },
            }
        } else {
            Ok(None)
        }
    }

    pub async fn revoke_refresh_token(&self, id: String) -> Result<bool> {
        auth::revoke_refresh_token(id, &self.db).await
    }

    // Invites
    // ----------

    pub async fn create_invite_codes(
        &self,
        to_create: Vec<AccountCodes>,
        use_count: i32,
    ) -> Result<()> {
        invite::create_invite_codes(to_create, use_count, &self.db).await
    }

    pub async fn create_account_invite_codes(
        &self,
        for_account: &str,
        codes: Vec<String>,
        expected_total: usize,
        disabled: bool,
    ) -> Result<Vec<CodeDetail>> {
        invite::create_account_invite_codes(for_account, codes, expected_total, disabled, &self.db)
            .await
    }

    pub async fn get_account_invite_codes(&self, did: &str) -> Result<Vec<CodeDetail>> {
        invite::get_account_invite_codes(did, &self.db).await
    }

    pub async fn get_invited_by_for_accounts(
        &self,
        dids: Vec<String>,
    ) -> Result<BTreeMap<String, CodeDetail>> {
        invite::get_invited_by_for_accounts(dids, &self.db).await
    }

    pub async fn set_account_invites_disabled(&self, did: &str, disabled: bool) -> Result<()> {
        invite::set_account_invites_disabled(did, disabled, &self.db).await
    }

    pub async fn disable_invite_codes(&self, opts: DisableInviteCodesOpts) -> Result<()> {
        invite::disable_invite_codes(opts, &self.db).await
    }

    // Passwords
    // ----------

    pub async fn create_app_password(
        &self,
        did: String,
        name: String,
    ) -> Result<CreateAppPasswordOutput> {
        password::create_app_password(did, name, &self.db).await
    }

    pub async fn list_app_passwords(&self, did: &str) -> Result<Vec<(String, String)>> {
        password::list_app_passwords(did, &self.db).await
    }

    pub async fn verify_account_password(&self, did: &str, password_str: &String) -> Result<bool> {
        password::verify_account_password(did, password_str, &self.db).await
    }

    pub async fn verify_app_password(
        &self,
        did: &str,
        password_str: &str,
    ) -> Result<Option<String>> {
        password::verify_app_password(did, password_str, &self.db).await
    }

    pub async fn reset_password(&self, opts: ResetPasswordOpts) -> Result<()> {
        let did = email_token::assert_valid_token_and_find_did(
            EmailTokenPurpose::ResetPassword,
            &opts.token,
            None,
            &self.db,
        )
        .await?;
        self.update_account_password(UpdateAccountPasswordOpts {
            did,
            password: opts.password,
        })
        .await
    }

    pub async fn update_account_password(&self, opts: UpdateAccountPasswordOpts) -> Result<()> {
        let UpdateAccountPasswordOpts { did, .. } = opts;
        let password_encrypted = password::gen_salt_and_hash(opts.password)?;
        try_join!(
            password::update_user_password(
                UpdateUserPasswordOpts {
                    did: did.clone(),
                    password_encrypted
                },
                &self.db
            ),
            email_token::delete_email_token(&did, EmailTokenPurpose::ResetPassword, &self.db),
            auth::revoke_refresh_tokens_by_did(&did, &self.db)
        )?;
        Ok(())
    }

    pub async fn revoke_app_password(&self, did: String, name: String) -> Result<()> {
        try_join!(
            password::delete_app_password(&did, &name, &self.db),
            auth::revoke_app_password_refresh_token(&did, &name, &self.db)
        )?;
        Ok(())
    }

    // Email Tokens
    // ----------
    pub async fn confirm_email(&self, opts: ConfirmEmailOpts<'_>) -> Result<()> {
        let ConfirmEmailOpts { did, token } = opts;
        email_token::assert_valid_token(
            did,
            EmailTokenPurpose::ConfirmEmail,
            token,
            None,
            &self.db,
        )
        .await?;
        let now = rsky_common::now();
        try_join!(
            email_token::delete_email_token(did, EmailTokenPurpose::ConfirmEmail, &self.db),
            account::set_email_confirmed_at(did, now, &self.db)
        )?;
        Ok(())
    }

    pub async fn update_email(&self, opts: UpdateEmailOpts) -> Result<()> {
        let UpdateEmailOpts { did, email } = opts;
        try_join!(
            account::update_email(&did, &email, &self.db),
            email_token::delete_all_email_tokens(&did, &self.db)
        )?;
        Ok(())
    }

    pub async fn assert_valid_email_token(
        &self,
        did: &str,
        purpose: EmailTokenPurpose,
        token: &str,
    ) -> Result<()> {
        email_token::assert_valid_token(did, purpose, token, None, &self.db).await
    }

    pub async fn assert_valid_email_token_and_cleanup(
        &self,
        did: &str,
        purpose: EmailTokenPurpose,
        token: &str,
    ) -> Result<()> {
        email_token::assert_valid_token(did, purpose, token, None, &self.db).await?;
        email_token::delete_email_token(did, purpose, &self.db).await
    }

    pub async fn create_email_token(
        &self,
        did: &str,
        purpose: EmailTokenPurpose,
    ) -> Result<String> {
        email_token::create_email_token(did, purpose, &self.db).await
    }
}

pub struct SharedAccountManager {
    pub account_manager: RwLock<AccountManagerCreator>,
}
