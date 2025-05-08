-- Drop indexes
DROP INDEX IF EXISTS idx_backlink_link_to;
DROP INDEX IF EXISTS idx_blob_tempkey;
DROP INDEX IF EXISTS idx_record_repo_rev;
DROP INDEX IF EXISTS idx_record_collection;
DROP INDEX IF EXISTS idx_record_cid;
DROP INDEX IF EXISTS idx_repo_block_repo_rev;

-- Drop tables
DROP TABLE IF EXISTS account_pref;
DROP TABLE IF EXISTS backlink;
DROP TABLE IF EXISTS record_blob;
DROP TABLE IF EXISTS blob;
DROP TABLE IF EXISTS record;
DROP TABLE IF EXISTS repo_block;
DROP TABLE IF EXISTS repo_root;
