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

CREATE TABLE localuser (
    id                      serial          NOT NULL PRIMARY KEY,
    local_user              varchar(100)    NOT NULL UNIQUE
);

CREATE TABLE projectname (
    id                      serial          NOT NULL PRIMARY KEY,
    project_name            varchar(200)    NOT NULL UNIQUE
);

CREATE TABLE submithost (
    id                      serial          NOT NULL PRIMARY KEY,
    submit_host             varchar(200)    NOT NULL UNIQUE
);

CREATE TABLE host (
    id                      serial          NOT NULL PRIMARY KEY,
    host                    varchar(1500)   NOT NULL UNIQUE
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
    local_user_id           integer         REFERENCES localuser (id),
    job_name                varchar(1000),
    charge                  integer,
    status_id               integer         REFERENCES jobstatus(id),
    queue_id                integer         REFERENCES jobqueue(id),
    host_id                 integer         REFERENCES host(id),
    node_count              smallint,
    processors              smallint,
    project_name_id         integer         REFERENCES projectname(id),
    submit_host_id          integer         REFERENCES submithost(id),
    start_time              timestamp,
    end_time                timestamp,
    submit_time             timestamp,
    cpu_duration            integer,
    wall_duration           integer,
    user_time               integer,
    kernel_time             integer,
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

CREATE TABLE hostscalefactors (
    machine_name            varchar(200)    NOT NULL UNIQUE PRIMARY KEY,
    scale_factor            float           NOT NULL
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
    size                    bigint,
    start_time              timestamp,
    end_time                timestamp,
    bypass_cache            boolean,
    retrieved_from_cache    boolean
);


-- this is the table used for storing aggregated usage information in
CREATE TABLE uraggregated_data (
    execution_time          date,
    insert_time             date,
    machine_name_id         integer,
    queue_id                integer,
    global_user_name_id     integer,
    local_user_id           integer,
    vo_information_id       integer,
    project_name_id         integer,
    runtime_environments_id integer[],
    status_id               integer,
    n_jobs                  integer,
    cputime                 bigint,
    walltime                bigint,
    generate_time           timestamp
);

-- this table is used for storing information about which parts
-- of the aggregartion table that needs to be updated
CREATE TABLE uraggregated_update (
    insert_time         date,
    machine_name_id     integer
);

