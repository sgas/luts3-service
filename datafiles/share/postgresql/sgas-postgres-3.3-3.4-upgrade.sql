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

DROP FUNCTION urcreate (character varying,
                        timestamp without time zone,
                        character varying,
                        character varying,
                        character varying,
                        character varying,
                        character varying,
                        character varying,
                        character varying,
                        character varying[],
                        character varying,
                        character varying,
                        numeric,
                        character varying,
                        character varying,
                        character varying,
                        integer,
                        integer,
                        character varying,
                        character varying,
                        timestamp without time zone,
                        timestamp without time zone,
                        timestamp without time zone,
                        numeric,
                        numeric,
                        numeric,
                        numeric,
                        integer,
                        integer,
                        integer,
                        character varying[],
                        integer,
                        character varying,
                        character varying,
                        timestamp without time zone);

SELECT 'View and functions dropped, you should reload them' AS Message;


