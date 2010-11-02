-- logic for upgrading the SGAS PostgreSQL schema from version 3.3 to 3.4
-- SGAS should be stopped when performing this upgrade


-- add hash index for insert_time in order to reduce the load on the system when performing updates

CREATE INDEX insert_time_date_hash_idx ON usagedata USING HASH (date(insert_time));


CREATE TYPE job_file_transfer_type AS ENUM ( 'download', 'upload' );


CREATE TABLE jobtransferurl (
    id                      serial          NOT NULL PRIMARY KEY,
    url                     varchar(2500)   NOT NULL UNIQUE
);


CREATE TABLE jobtransferdata (
    id                      serial          NOT NULL PRIMARY KEY,
    usage_data_id           integer         REFERENCES usagedata (id),
    job_transfer_url_id     integer         REFERENCES jobtransferurl (id),
    transfer_type           job_file_transfer_type  NOT NULL,
    size                    integer,
    start_time              timestamp,
    end_time                timestamp,
    bypass_cache            boolean,
    retrieved_from_cache    boolean
);

-- drop old view and functions
DROP VIEW usagerecords;

-- FIXME missing arguments
-- DROP FUNCTION urcreate ( );

SELECT 'View and functions dropped, you should reload them' AS Message;


