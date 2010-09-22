-- logic for upgrading the SGAS PostgreSQL schema from version 3.3 to 3.4
-- SGAS should be stopped when performing this upgrade


-- add hash index for insert_time in order to reduce the load on the system when performing updates

CREATE INDEX insert_time_date_hash_idx ON usagedata USING HASH (date(insert_time));


