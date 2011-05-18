-- upgrade sgas luts schema from version 3.5.x to 3.6.0
-- the changes are the addition of a storage record schema

-- changes to usage record part

DROP VIEW usagerecords;
DROP VIEW transfers;
DROP VIEW uraggregated;

DROP FUNCTION urcreate (character varying, timestamp without time zone, character varying, character varying,
                        character varying, character varying, character varying, character varying, character varying,
                        character varying[], character varying, character varying, integer, character varying,
                        character varying, character varying, integer, integer, character varying, character varying,
                        timestamp without time zone, timestamp without time zone, timestamp without time zone,
                        integer, integer, integer, integer, integer, character varying[], integer,
                        character varying[], character varying[], character varying, character varying,
                        timestamp without time zone);

DROP FUNCTION update_uraggregate();

ALTER TABLE host ALTER COLUMN host TYPE varchar(7000);

ALTER TABLE uraggregated_data ADD COLUMN insert_host_id integer;


-- new storage record schema

CREATE TABLE storagesystem (
    id                      serial          NOT NULL UNIQUE PRIMARY KEY,
    storage_system          varchar(1000)   NOT NULL UNIQUE
);


CREATE TABLE storageshare (
    id                      serial          NOT NULL UNIQUE PRIMARY KEY,
    storage_share           varchar(1000)   NOT NULL UNIQUE
);


CREATE TABLE storagemedia (
    id                      serial          NOT NULL UNIQUE PRIMARY KEY,
    storage_media           varchar(1000)   NOT NULL UNIQUE
);


CREATE TABLE storageclass (
    id                      serial          NOT NULL UNIQUE PRIMARY KEY,
    storage_class           varchar(1000)   NOT NULL UNIQUE
);


CREATE TABLE directorypath (
    id                      serial          NOT NULL UNIQUE PRIMARY KEY,
    directory_path          varchar(2000)   NOT NULL UNIQUE
);


CREATE TABLE localgroup (
    id                      serial          NOT NULL UNIQUE PRIMARY KEY,
    local_group             varchar(1000)   NOT NULL UNIQUE
);


CREATE TABLE useridentity (
    id                      serial          NOT NULL UNIQUE PRIMARY KEY,
    user_identity           varchar(2000)   NOT NULL UNIQUE
);


-- group is a reserved word, so we use group identity
CREATE TABLE groupidentity (
    id                      serial          NOT NULL UNIQUE PRIMARY KEY,
    group_identity          varchar(1000)   NOT NULL,
    group_attribute         varchar(100)[][]
);


CREATE TABLE storagedata (
    id                      serial          NOT NULL UNIQUE PRIMARY KEY,
    record_id               varchar(1000)   NOT NULL UNIQUE,
    create_time             timestamp       NOT NULL,
    storage_system_id       integer         NOT NULL REFERENCES storagesystem(id),
    storage_share_id        integer         REFERENCES storageshare(id),
    storage_media_id        integer         REFERENCES storagemedia(id),
    storage_class_id        integer         REFERENCES storageshare(id),
    file_count              integer,
    directory_path_id       integer         REFERENCES directorypath(id),
    local_user_id           integer         REFERENCES localuser(id),
    local_group_id          integer         REFERENCES localgroup(id),
    user_identity_id        integer         REFERENCES useridentity(id),
    group_identity_id       integer         REFERENCES groupidentity(id),
    measure_time            timestamp       NOT NULL,
    valid_duration          integer,        -- seconds
    resource_capacity_used  bigint          NOT NULL,
    logical_capacity_used   bigint,
    insert_host_id          integer         REFERENCES inserthost (id),
    insert_identity_id      integer         REFERENCES insertidentity (id),
    insert_time             timestamp
);

-- srcreate function is loaded from functions file.


SELECT 'Views and functions dropped, you should reload them' AS Message;
SELECT 'Aggregation data changed, aggregation table should be rebuild.' AS Message;

