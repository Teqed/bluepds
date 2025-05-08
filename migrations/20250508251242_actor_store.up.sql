-- Actor store schema matching TypeScript implementation

-- Repository root information
CREATE TABLE IF NOT EXISTS repo_root (
    did TEXT PRIMARY KEY NOT NULL,
    cid TEXT NOT NULL,
    rev TEXT NOT NULL,
    indexedAt TEXT NOT NULL
);

-- Repository blocks (IPLD blocks)
CREATE TABLE IF NOT EXISTS repo_block (
    cid TEXT PRIMARY KEY NOT NULL,
    repoRev TEXT NOT NULL,
    size INTEGER NOT NULL,
    content BLOB NOT NULL
);

-- Record index
CREATE TABLE IF NOT EXISTS record (
    uri TEXT PRIMARY KEY NOT NULL,
    cid TEXT NOT NULL,
    collection TEXT NOT NULL,
    rkey TEXT NOT NULL,
    repoRev TEXT NOT NULL,
    indexedAt TEXT NOT NULL,
    takedownRef TEXT
);

-- Blob storage metadata
CREATE TABLE IF NOT EXISTS blob (
    cid TEXT PRIMARY KEY NOT NULL,
    mimeType TEXT NOT NULL,
    size INTEGER NOT NULL,
    tempKey TEXT,
    width INTEGER,
    height INTEGER,
    createdAt TEXT NOT NULL,
    takedownRef TEXT
);

-- Record-blob associations
CREATE TABLE IF NOT EXISTS record_blob (
    blobCid TEXT NOT NULL,
    recordUri TEXT NOT NULL,
    PRIMARY KEY (blobCid, recordUri)
);

-- Backlinks between records
CREATE TABLE IF NOT EXISTS backlink (
    uri TEXT NOT NULL,
    path TEXT NOT NULL,
    linkTo TEXT NOT NULL,
    PRIMARY KEY (uri, path)
);

-- User preferences
CREATE TABLE IF NOT EXISTS account_pref (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    valueJson TEXT NOT NULL
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_repo_block_repo_rev ON repo_block(repoRev, cid);
CREATE INDEX IF NOT EXISTS idx_record_cid ON record(cid);
CREATE INDEX IF NOT EXISTS idx_record_collection ON record(collection);
CREATE INDEX IF NOT EXISTS idx_record_repo_rev ON record(repoRev);
CREATE INDEX IF NOT EXISTS idx_blob_tempkey ON blob(tempKey);
CREATE INDEX IF NOT EXISTS idx_backlink_link_to ON backlink(path, linkTo);
