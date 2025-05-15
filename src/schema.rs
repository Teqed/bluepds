#![allow(unnameable_types, unused_qualifications)]
pub mod pds {

    // Legacy tables

    diesel::table! {
        pds.oauth_par_requests (request_uri) {
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
        pds.oauth_authorization_codes (code) {
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
        pds.oauth_refresh_tokens (token) {
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
        pds.oauth_used_jtis (jti) {
            jti -> Varchar,
            issuer -> Varchar,
            created_at -> Int8,
            expires_at -> Int8,
        }
    }

    // Upcoming tables

    diesel::table! {
        pds.authorization_request (id) {
            id -> Varchar,
            did -> Nullable<Varchar>,
            deviceId -> Nullable<Varchar>,
            clientId -> Varchar,
            clientAuth -> Varchar,
            parameters -> Varchar,
            expiresAt -> Timestamptz,
            code -> Nullable<Varchar>,
        }
    }

    diesel::table! {
        pds.device (id) {
            id -> Varchar,
            sessionId -> Nullable<Varchar>,
            userAgent -> Nullable<Varchar>,
            ipAddress -> Varchar,
            lastSeenAt -> Timestamptz,
        }
    }

    diesel::table! {
        pds.device_account (deviceId, did) {
            did -> Varchar,
            deviceId -> Varchar,
            authenticatedAt -> Timestamptz,
            remember -> Bool,
            authorizedClients -> Varchar,
        }
    }

    diesel::table! {
        pds.token (id) {
            id -> Varchar,
            did -> Varchar,
            tokenId -> Varchar,
            createdAt -> Timestamptz,
            updatedAt -> Timestamptz,
            expiresAt -> Timestamptz,
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
        pds.used_refresh_token (refreshToken) {
            refreshToken -> Varchar,
            tokenId -> Varchar,
        }
    }
}
