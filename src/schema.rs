#![allow(unnameable_types, unused_qualifications)]
pub mod pds {

    // Legacy tables

    diesel::table! {
        oauth_par_requests (request_uri) {
            request_uri -> Varchar,
            client_id -> Varchar,
            response_type -> Varchar,
            code_challenge -> Varchar,
            code_challenge_method -> Varchar,
            state -> Nullable<Varchar>,
            login_hint -> Nullable<Varchar>,
            scope -> Nullable<Varchar>,
            redirect_uri -> Nullable<Varchar>,
            response_mode -> Nullable<Varchar>,
            display -> Nullable<Varchar>,
            created_at -> Int8,
            expires_at -> Int8,
        }
    }
    diesel::table! {
        oauth_authorization_codes (code) {
            code -> Varchar,
            client_id -> Varchar,
            subject -> Varchar,
            code_challenge -> Varchar,
            code_challenge_method -> Varchar,
            redirect_uri -> Varchar,
            scope -> Nullable<Varchar>,
            created_at -> Int8,
            expires_at -> Int8,
            used -> Bool,
        }
    }
    diesel::table! {
        oauth_refresh_tokens (token) {
            token -> Varchar,
            client_id -> Varchar,
            subject -> Varchar,
            dpop_thumbprint -> Varchar,
            scope -> Nullable<Varchar>,
            created_at -> Int8,
            expires_at -> Int8,
            revoked -> Bool,
        }
    }
    diesel::table! {
        oauth_used_jtis (jti) {
            jti -> Varchar,
            issuer -> Varchar,
            created_at -> Int8,
            expires_at -> Int8,
        }
    }

    // Upcoming tables

    diesel::table! {
        account (did) {
            did -> Varchar,
            email -> Varchar,
            recoveryKey -> Nullable<Varchar>,
            password -> Varchar,
            createdAt -> Varchar,
            invitesDisabled -> Int2,
            emailConfirmedAt -> Nullable<Varchar>,
        }
    }

    diesel::table! {
        account_pref (id) {
            id -> Int4,
            did -> Varchar,
            name -> Varchar,
            valueJson -> Nullable<Text>,
        }
    }

    diesel::table! {
        actor (did) {
            did -> Varchar,
            handle -> Nullable<Varchar>,
            createdAt -> Varchar,
            takedownRef -> Nullable<Varchar>,
            deactivatedAt -> Nullable<Varchar>,
            deleteAfter -> Nullable<Varchar>,
        }
    }

    diesel::table! {
        app_password (did, name) {
            did -> Varchar,
            name -> Varchar,
            password -> Varchar,
            createdAt -> Varchar,
        }
    }

    diesel::table! {
        authorization_request (id) {
            id -> Varchar,
            did -> Nullable<Varchar>,
            deviceId -> Nullable<Varchar>,
            clientId -> Varchar,
            clientAuth -> Varchar,
            parameters -> Varchar,
            expiresAt -> TimestamptzSqlite,
            code -> Nullable<Varchar>,
        }
    }

    diesel::table! {
        backlink (uri, path) {
            uri -> Varchar,
            path -> Varchar,
            linkTo -> Varchar,
        }
    }

    diesel::table! {
        blob (cid, did) {
            cid -> Varchar,
            did -> Varchar,
            mimeType -> Varchar,
            size -> Int4,
            tempKey -> Nullable<Varchar>,
            width -> Nullable<Int4>,
            height -> Nullable<Int4>,
            createdAt -> Varchar,
            takedownRef -> Nullable<Varchar>,
        }
    }

    diesel::table! {
        device (id) {
            id -> Varchar,
            sessionId -> Nullable<Varchar>,
            userAgent -> Nullable<Varchar>,
            ipAddress -> Varchar,
            lastSeenAt -> TimestamptzSqlite,
        }
    }

    diesel::table! {
        device_account (deviceId, did) {
            did -> Varchar,
            deviceId -> Varchar,
            authenticatedAt -> TimestamptzSqlite,
            remember -> Bool,
            authorizedClients -> Varchar,
        }
    }

    diesel::table! {
        did_doc (did) {
            did -> Varchar,
            doc -> Text,
            updatedAt -> Int8,
        }
    }

    diesel::table! {
        email_token (purpose, did) {
            purpose -> Varchar,
            did -> Varchar,
            token -> Varchar,
            requestedAt -> Varchar,
        }
    }

    diesel::table! {
        invite_code (code) {
            code -> Varchar,
            availableUses -> Int4,
            disabled -> Int2,
            forAccount -> Varchar,
            createdBy -> Varchar,
            createdAt -> Varchar,
        }
    }

    diesel::table! {
        invite_code_use (code, usedBy) {
            code -> Varchar,
            usedBy -> Varchar,
            usedAt -> Varchar,
        }
    }

    diesel::table! {
        record (uri) {
            uri -> Varchar,
            cid -> Varchar,
            did -> Varchar,
            collection -> Varchar,
            rkey -> Varchar,
            repoRev -> Nullable<Varchar>,
            indexedAt -> Varchar,
            takedownRef -> Nullable<Varchar>,
        }
    }

    diesel::table! {
        record_blob (blobCid, recordUri) {
            blobCid -> Varchar,
            recordUri -> Varchar,
            did -> Varchar,
        }
    }

    diesel::table! {
        refresh_token (id) {
            id -> Varchar,
            did -> Varchar,
            expiresAt -> Varchar,
            nextId -> Nullable<Varchar>,
            appPasswordName -> Nullable<Varchar>,
        }
    }

    diesel::table! {
        repo_block (cid, did) {
            cid -> Varchar,
            did -> Varchar,
            repoRev -> Varchar,
            size -> Int4,
            content -> Bytea,
        }
    }

    diesel::table! {
        repo_root (did) {
            did -> Varchar,
            cid -> Varchar,
            rev -> Varchar,
            indexedAt -> Varchar,
        }
    }

    diesel::table! {
        repo_seq (seq) {
            seq -> Int8,
            did -> Varchar,
            eventType -> Varchar,
            event -> Bytea,
            invalidated -> Int2,
            sequencedAt -> Varchar,
        }
    }

    diesel::table! {
        token (id) {
            id -> Varchar,
            did -> Varchar,
            tokenId -> Varchar,
            createdAt -> TimestamptzSqlite,
            updatedAt -> TimestamptzSqlite,
            expiresAt -> TimestamptzSqlite,
            clientId -> Varchar,
            clientAuth -> Varchar,
            deviceId -> Nullable<Varchar>,
            parameters -> Varchar,
            details -> Nullable<Varchar>,
            code -> Nullable<Varchar>,
            currentRefreshToken -> Nullable<Varchar>,
        }
    }

    diesel::table! {
        used_refresh_token (refreshToken) {
            refreshToken -> Varchar,
            tokenId -> Varchar,
        }
    }

    diesel::allow_tables_to_appear_in_same_query!(
        account,
        account_pref,
        actor,
        app_password,
        authorization_request,
        backlink,
        blob,
        device,
        device_account,
        did_doc,
        email_token,
        invite_code,
        invite_code_use,
        record,
        record_blob,
        refresh_token,
        repo_block,
        repo_root,
        repo_seq,
        token,
        used_refresh_token,
    );
}
