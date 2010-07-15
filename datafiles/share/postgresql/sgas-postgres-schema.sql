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

CREATE TABLE insertidentity (
    id                      serial          NOT NULL PRIMARY KEY,
    insert_identity         varchar(1024)   NOT NULL UNIQUE
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
    status                  varchar(100),
    queue                   varchar(200),
    host                    varchar(500),
    node_count              integer,
    project_name            varchar(200),
    submit_host             varchar(200),
    start_time              timestamp,
    end_time                timestamp,
    submit_time             timestamp,
    cpu_duration            numeric(12,2),
    wall_duration           numeric(12,2),
    ksi2k_cpu_duration      numeric(12,2),
    ksi2k_wall_duration     numeric(12,2),
    user_time               numeric(12,2),
    kernel_time             numeric(12,2),
    major_page_faults       integer,
    runtime_environments    varchar(512)[],
    exit_code               integer,
    insert_hostname         varchar(1024),
    insert_identity_id      integer         REFERENCES insertidentity (id),
    insert_time             timestamp
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


-- create view of the usage records in the database
-- the purpose of the view is to make data easily accessable so users do not
-- have to do the joins between the tables them selves.
CREATE VIEW usagerecords AS
SELECT
    record_id,
    create_time,
    globalusername.global_user_name,
    voinformation.vo_type,
    voinformation.vo_issuer,
    voinformation.vo_name,
    voinformation.vo_attributes,
    machinename.machine_name,
    global_job_id,
    local_job_id,
    local_user_id,
    job_name,
    charge,
    status,
    queue,
    host,
    node_count,
    project_name,
    submit_host,
    start_time,
    end_time,
    submit_time,
    cpu_duration,
    wall_duration,
    ksi2k_cpu_duration,
    ksi2k_wall_duration,
    user_time,
    kernel_time,
    major_page_faults,
    runtime_environments,
    exit_code,
    insert_hostname,
    insertidentity.insert_identity,
    insert_time
FROM
    usagedata
LEFT OUTER JOIN globalusername  ON (usagedata.global_user_name_id = globalusername.id)
LEFT OUTER JOIN voinformation   ON (usagedata.vo_information_id   = voinformation.id)
LEFT OUTER JOIN machinename     ON (usagedata.machine_name_id     = machinename.id)
LEFT OUTER JOIN insertidentity  ON (usagedata.insert_identity_id  = insertidentity.id)
;

