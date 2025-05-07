-- Store raw blocks with their CIDs
CREATE TABLE IF NOT EXISTS blocks (
    cid TEXT PRIMARY KEY NOT NULL,
    data BLOB NOT NULL,
    multicodec INTEGER NOT NULL,
    multihash INTEGER NOT NULL
);

-- Store the repository tree structure
CREATE TABLE IF NOT EXISTS tree_nodes (
    repo_did TEXT NOT NULL,
    key TEXT NOT NULL,
    value_cid TEXT NOT NULL,
    PRIMARY KEY (repo_did, key),
    FOREIGN KEY (value_cid) REFERENCES blocks(cid)
);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_blocks_cid ON blocks(cid);
CREATE INDEX IF NOT EXISTS idx_tree_nodes_repo ON tree_nodes(repo_did);
