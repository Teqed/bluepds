CREATE TABLE `blob`(
	`cid` VARCHAR NOT NULL,
	`did` VARCHAR NOT NULL,
	`mimetype` VARCHAR NOT NULL,
	`size` INT4 NOT NULL,
	`tempkey` VARCHAR,
	`width` INT4,
	`height` INT4,
	`createdat` VARCHAR NOT NULL,
	`takedownref` VARCHAR,
	PRIMARY KEY(`cid`, `did`)
);

CREATE TABLE `repo_block`(
	`cid` VARCHAR NOT NULL,
	`did` VARCHAR NOT NULL,
	`reporev` VARCHAR NOT NULL,
	`size` INT4 NOT NULL,
	`content` BYTEA NOT NULL,
	PRIMARY KEY(`cid`, `did`)
);

CREATE TABLE `backlink`(
	`uri` VARCHAR NOT NULL,
	`path` VARCHAR NOT NULL,
	`linkto` VARCHAR NOT NULL,
	PRIMARY KEY(`uri`, `path`)
);

CREATE TABLE `record`(
	`uri` VARCHAR NOT NULL PRIMARY KEY,
	`cid` VARCHAR NOT NULL,
	`did` VARCHAR NOT NULL,
	`collection` VARCHAR NOT NULL,
	`rkey` VARCHAR NOT NULL,
	`reporev` VARCHAR,
	`indexedat` VARCHAR NOT NULL,
	`takedownref` VARCHAR
);

CREATE TABLE `repo_root`(
	`did` VARCHAR NOT NULL PRIMARY KEY,
	`cid` VARCHAR NOT NULL,
	`rev` VARCHAR NOT NULL,
	`indexedat` VARCHAR NOT NULL
);

CREATE TABLE `account_pref`(
	`id` INT4 NOT NULL PRIMARY KEY,
	`did` VARCHAR NOT NULL,
	`name` VARCHAR NOT NULL,
	`valuejson` TEXT
);

CREATE TABLE `record_blob`(
	`blobcid` VARCHAR NOT NULL,
	`recorduri` VARCHAR NOT NULL,
	`did` VARCHAR NOT NULL,
	PRIMARY KEY(`blobCid`, `recordUri`)
);
