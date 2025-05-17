CREATE TABLE `repo_seq`(
	`seq` INT8 NOT NULL PRIMARY KEY,
	`did` VARCHAR NOT NULL,
	`eventtype` VARCHAR NOT NULL,
	`event` BYTEA NOT NULL,
	`invalidated` INT2 NOT NULL,
	`sequencedat` VARCHAR NOT NULL
);

CREATE TABLE `app_password`(
	`did` VARCHAR NOT NULL,
	`name` VARCHAR NOT NULL,
	`password` VARCHAR NOT NULL,
	`createdat` VARCHAR NOT NULL,
	PRIMARY KEY(`did`, `name`)
);

CREATE TABLE `device_account`(
	`did` VARCHAR NOT NULL,
	`deviceid` VARCHAR NOT NULL,
	`authenticatedat` TIMESTAMPTZ NOT NULL,
	`remember` BOOL NOT NULL,
	`authorizedclients` VARCHAR NOT NULL,
	PRIMARY KEY(`deviceId`, `did`)
);

CREATE TABLE `actor`(
	`did` VARCHAR NOT NULL PRIMARY KEY,
	`handle` VARCHAR,
	`createdat` VARCHAR NOT NULL,
	`takedownref` VARCHAR,
	`deactivatedat` VARCHAR,
	`deleteafter` VARCHAR
);

CREATE TABLE `device`(
	`id` VARCHAR NOT NULL PRIMARY KEY,
	`sessionid` VARCHAR,
	`useragent` VARCHAR,
	`ipaddress` VARCHAR NOT NULL,
	`lastseenat` TIMESTAMPTZ NOT NULL
);

CREATE TABLE `did_doc`(
	`did` VARCHAR NOT NULL PRIMARY KEY,
	`doc` TEXT NOT NULL,
	`updatedat` INT8 NOT NULL
);

CREATE TABLE `email_token`(
	`purpose` VARCHAR NOT NULL,
	`did` VARCHAR NOT NULL,
	`token` VARCHAR NOT NULL,
	`requestedat` VARCHAR NOT NULL,
	PRIMARY KEY(`purpose`, `did`)
);

CREATE TABLE `invite_code`(
	`code` VARCHAR NOT NULL PRIMARY KEY,
	`availableuses` INT4 NOT NULL,
	`disabled` INT2 NOT NULL,
	`foraccount` VARCHAR NOT NULL,
	`createdby` VARCHAR NOT NULL,
	`createdat` VARCHAR NOT NULL
);

CREATE TABLE `used_refresh_token`(
	`refreshtoken` VARCHAR NOT NULL PRIMARY KEY,
	`tokenid` VARCHAR NOT NULL
);

CREATE TABLE `invite_code_use`(
	`code` VARCHAR NOT NULL,
	`usedby` VARCHAR NOT NULL,
	`usedat` VARCHAR NOT NULL,
	PRIMARY KEY(`code`, `usedBy`)
);

CREATE TABLE `authorization_request`(
	`id` VARCHAR NOT NULL PRIMARY KEY,
	`did` VARCHAR,
	`deviceid` VARCHAR,
	`clientid` VARCHAR NOT NULL,
	`clientauth` VARCHAR NOT NULL,
	`parameters` VARCHAR NOT NULL,
	`expiresat` TIMESTAMPTZ NOT NULL,
	`code` VARCHAR
);

CREATE TABLE `token`(
	`id` VARCHAR NOT NULL PRIMARY KEY,
	`did` VARCHAR NOT NULL,
	`tokenid` VARCHAR NOT NULL,
	`createdat` TIMESTAMPTZ NOT NULL,
	`updatedat` TIMESTAMPTZ NOT NULL,
	`expiresat` TIMESTAMPTZ NOT NULL,
	`clientid` VARCHAR NOT NULL,
	`clientauth` VARCHAR NOT NULL,
	`deviceid` VARCHAR,
	`parameters` VARCHAR NOT NULL,
	`details` VARCHAR,
	`code` VARCHAR,
	`currentrefreshtoken` VARCHAR
);

CREATE TABLE `refresh_token`(
	`id` VARCHAR NOT NULL PRIMARY KEY,
	`did` VARCHAR NOT NULL,
	`expiresat` VARCHAR NOT NULL,
	`nextid` VARCHAR,
	`apppasswordname` VARCHAR
);

CREATE TABLE `account`(
	`did` VARCHAR NOT NULL PRIMARY KEY,
	`email` VARCHAR NOT NULL,
	`recoverykey` VARCHAR,
	`password` VARCHAR NOT NULL,
	`createdat` VARCHAR NOT NULL,
	`invitesdisabled` INT2 NOT NULL,
	`emailconfirmedat` VARCHAR
);
