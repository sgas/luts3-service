-- logic for upgrading the SGAS PostgreSQL schema from version 3.4.0 to 3.4.1
-- SGAS should be stopped when performing this upgrade


-- drop views so we can perform changes (only transfers really needed)
DROP VIEW usagerecords;
DROP VIEW transfers;

-- alter file size to be 8 byte integer instead of 4 byte
ALTER TABLE jobtransferdata ALTER size TYPE bigint;

-- drop functions, need to accmodate bigint transition (only urcreate really needed)
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
                        integer, integer,
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
                        character varying[],
                        character varying[],
                        character varying,
                        character varying,
                        timestamp without time zone);

DROP FUNCTION update_uraggregate();

SELECT 'View and functions dropped, you should reload them' AS Message;

