CREATE TABLE `oauth_refresh_tokens`(
	`token` VARCHAR NOT NULL PRIMARY KEY,
	`client_id` VARCHAR NOT NULL,
	`subject` VARCHAR NOT NULL,
	`dpop_thumbprint` VARCHAR NOT NULL,
	`scope` VARCHAR,
	`created_at` INT8 NOT NULL,
	`expires_at` INT8 NOT NULL,
	`revoked` BOOL NOT NULL
);

CREATE TABLE `oauth_used_jtis`(
	`jti` VARCHAR NOT NULL PRIMARY KEY,
	`issuer` VARCHAR NOT NULL,
	`created_at` INT8 NOT NULL,
	`expires_at` INT8 NOT NULL
);

CREATE TABLE `oauth_par_requests`(
	`request_uri` VARCHAR NOT NULL PRIMARY KEY,
	`client_id` VARCHAR NOT NULL,
	`response_type` VARCHAR NOT NULL,
	`code_challenge` VARCHAR NOT NULL,
	`code_challenge_method` VARCHAR NOT NULL,
	`state` VARCHAR,
	`login_hint` VARCHAR,
	`scope` VARCHAR,
	`redirect_uri` VARCHAR,
	`response_mode` VARCHAR,
	`display` VARCHAR,
	`created_at` INT8 NOT NULL,
	`expires_at` INT8 NOT NULL
);

CREATE TABLE `oauth_authorization_codes`(
	`code` VARCHAR NOT NULL PRIMARY KEY,
	`client_id` VARCHAR NOT NULL,
	`subject` VARCHAR NOT NULL,
	`code_challenge` VARCHAR NOT NULL,
	`code_challenge_method` VARCHAR NOT NULL,
	`redirect_uri` VARCHAR NOT NULL,
	`scope` VARCHAR,
	`created_at` INT8 NOT NULL,
	`expires_at` INT8 NOT NULL,
	`used` BOOL NOT NULL
);
