CREATE TABLE IF NOT EXISTS blob_ref (
    -- N.B: There is a hidden `rowid` field inserted by sqlite.
    cid TEXT NOT NULL,
    did TEXT NOT NULL,
    record TEXT
);
