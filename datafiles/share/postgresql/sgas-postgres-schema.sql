-- SGAS PostgreSQL schema
-- The schema is not really designed to follow any normal form, though it does
-- try to minimize duplicate information by putting out "common" information
-- into seperate tables. The schema is a star-schema, which are typically
-- fairly good for data mining (but consider using the uraggregate view)


CREATE TABLE globalusername (
    id                      serial          NOT NULL PRIMARY KEY,
    global_user_name        varchar(1000)   NOT NULL UNIQUE
);

CREATE TABLE voinformation (
    id                      serial          NOT NULL PRIMARY KEY,
    vo_type                 varchar(100),
    vo_issuer               varchar(1000),
    vo_name                 varchar(1000)   NOT NULL,
    vo_attributes           varchar(100)[][]
);

CREATE TABLE machinename (
    id                      serial          NOT NULL PRIMARY KEY,
    machine_name            varchar(200)    NOT NULL UNIQUE
);

CREATE TABLE jobstatus (
    id                      serial          NOT NULL PRIMARY KEY,
    status                  varchar(100)    NOT NULL UNIQUE
);

CREATE TABLE jobqueue (
    id                      serial          NOT NULL PRIMARY KEY,
    queue                   varchar(200)    NOT NULL UNIQUE
);

CREATE TABLE insertidentity (
    id                      serial          NOT NULL PRIMARY KEY,
    insert_identity         varchar(1024)   NOT NULL UNIQUE
);

CREATE TABLE inserthost (
    id                      serial          NOT NULL PRIMARY KEY,
    insert_host             varchar(1024)   NOT NULL UNIQUE
);

CREATE TABLE usagedata (
    id                      serial          NOT NULL PRIMARY KEY,
    record_id               varchar(1000)   NOT NULL UNIQUE,
    create_time             timestamp       NOT NULL,
    global_user_name_id     integer         REFERENCES globalusername (id),
    vo_information_id       integer         REFERENCES voinformation (id),
    machine_name_id         integer         REFERENCES machinename (id),
    global_job_id           varchar(1000),
    local_job_id            varchar(500),
    local_user_id           varchar(100),
    job_name                varchar(1000),
    charge                  numeric(12,2),
    status_id               integer         REFERENCES jobstatus(id),
    queue_id                integer         REFERENCES jobqueue(id),
    host                    varchar(1500),
    node_count              smallint,
    processors              smallint,
    project_name            varchar(200),
    submit_host             varchar(200),
    start_time              timestamp,
    end_time                timestamp,
    submit_time             timestamp,
    cpu_duration            numeric(12,2),
    wall_duration           numeric(12,2),
    user_time               numeric(12,2),
    kernel_time             numeric(12,2),
    major_page_faults       integer,
    exit_code               smallint,
    insert_host_id          integer         REFERENCES inserthost (id),
    insert_identity_id      integer         REFERENCES insertidentity (id),
    insert_time             timestamp
);

CREATE TABLE runtimeenvironment (
    id                      serial          NOT NULL PRIMARY KEY,
    runtime_environment     varchar(512)    NOT NULL UNIQUE
);

CREATE TABLE runtimeenvironment_usagedata (
    usagedata_id            integer         NOT NULL REFERENCES usagedata (id),
    runtimeenvironments_id  integer         NOT NULL REFERENCES runtimeenvironment(id),
    PRIMARY KEY (usagedata_id, runtimeenvironments_id)
);


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


-- this is the table used for storing aggregated usage information in
CREATE TABLE uraggregated (
    execution_time      date,
    insert_time         date,
    machine_name        varchar,
    user_identity       varchar,
    vo_issuer           varchar,
    vo_name             varchar,
    vo_group            varchar,
    vo_role             varchar,
    n_jobs              integer,
    cputime             numeric(14,2),
    walltime            numeric(14,2),
    generate_time       timestamp
);

-- this table is used for storing information about which parts
-- of the aggregartion table that needs to be updated
CREATE TABLE uraggregated_update (
    insert_time         date,
    machine_name        varchar
);

