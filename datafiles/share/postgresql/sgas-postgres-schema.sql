-- SGAS PostgreSQL schema
-- The schema is not really designed to follow any normal form, though it does
-- try to minimize duplicate information by putting out "common" information
-- into seperate tables. The schema is a star-schema, which are typically
-- fairly good for data mining


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
    insert_identity         varchar(1024)   NOT NULL
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
    n_jobs               integer,
    cputime             integer,
    walltime            integer,
    generate_time       timestamp
);

-- this table is used for storing information about which parts
-- of the aggregartion table that needs to be updated
CREATE TABLE uraggregated_update (
    insert_time         date,
    machine_name        varchar
);


-- embedded function for inserting usage records
CREATE OR REPLACE FUNCTION urcreate (
    in_record_id               varchar,
    in_create_time             timestamp,
    in_global_job_id           varchar     DEFAULT NULL,
    in_local_job_id            varchar     DEFAULT NULL,
    in_local_user_id           varchar     DEFAULT NULL,
    in_global_user_name        varchar     DEFAULT NULL,
    in_vo_type                 varchar     DEFAULT NULL,
    in_vo_issuer               varchar     DEFAULT NULL,
    in_vo_name                 varchar     DEFAULT NULL,
    in_vo_attributes           varchar[][] DEFAULT NULL,
    in_machine_name            varchar     DEFAULT NULL,
    in_job_name                varchar     DEFAULT NULL,
    in_charge                  numeric     DEFAULT NULL,
    in_status                  varchar     DEFAULT NULL,
    in_queue                   varchar     DEFAULT NULL,
    in_host                    varchar     DEFAULT NULL,
    in_node_count              integer     DEFAULT NULL,
    in_project_name            varchar     DEFAULT NULL,
    in_submit_host             varchar     DEFAULT NULL,
    in_start_time              timestamp   DEFAULT NULL,
    in_end_time                timestamp   DEFAULT NULL,
    in_submit_time             timestamp   DEFAULT NULL,
    in_cpu_duration            numeric     DEFAULT NULL,
    in_wall_duration           numeric     DEFAULT NULL,
    in_ksi2k_cpu_duration      numeric     DEFAULT NULL,
    in_ksi2k_wall_duration     numeric     DEFAULT NULL,
    in_user_time               integer     DEFAULT NULL,
    in_kernel_time             integer     DEFAULT NULL,
    in_major_page_faults       integer     DEFAULT NULL,
    in_runtime_environments    varchar[]   DEFAULT NULL,
    in_exit_code               integer     DEFAULT NULL,
    in_insert_hostname         varchar     DEFAULT NULL,
    in_insert_identity         varchar     DEFAULT NULL,
    in_insert_time             timestamp   DEFAULT NULL
)
RETURNS varchar[] AS $recordid_rowid$

DECLARE
    globalusername_id       integer;
    voinformation_id        integer;
    machinename_id          integer;
    insertidentity_id       integer;
    usagerecord_id          integer;
    result                  varchar[];
BEGIN
    -- first check that we do not have the record already
    SELECT INTO usagerecord_id id
           FROM usagedata
           WHERE record_id = in_record_id;
    IF FOUND THEN
        result[0] = in_record_id;
        result[1] = usagerecord_id;
        RETURN result;
    END IF;

    -- global user name
    IF in_global_user_name IS NULL THEN
        globalusername_id = NULL;
    ELSE
        SELECT INTO globalusername_id id
               FROM globalusername
               WHERE global_user_name = in_global_user_name;
        IF NOT FOUND THEN
            INSERT INTO globalusername (global_user_name)
                VALUES (in_global_user_name) RETURNING id INTO globalusername_id;
        END IF;
    END IF;

    -- vo information
    IF in_vo_name is NULL THEN
        voinformation_id = NULL;
    ELSE
        SELECT INTO voinformation_id id
               FROM voinformation
               WHERE vo_type        IS NOT DISTINCT FROM in_vo_type AND
                     vo_issuer      IS NOT DISTINCT FROM in_vo_issuer AND
                     vo_name        IS NOT DISTINCT FROM in_vo_name AND
                     vo_attributes  IS NOT DISTINCT FROM in_vo_attributes;
        IF NOT FOUND THEN
            INSERT INTO voinformation (vo_type, vo_issuer, vo_name, vo_attributes)
                   VALUES (in_vo_type, in_vo_issuer, in_vo_name, in_vo_attributes) RETURNING id INTO voinformation_id;
        END IF;
    END IF;

    -- machine name
    IF in_machine_name IS NULL THEN
        machinename_id = NULL;
    ELSE
        SELECT INTO machinename_id id
               FROM machinename
               WHERE machine_name = in_machine_name;
        IF NOT FOUND THEN
            INSERT INTO machinename (machine_name)
                   VALUES (in_machine_name) RETURNING id INTO machinename_id;
        END IF;
    END IF;

    -- insert identity
    IF in_insert_identity IS NULL THEN
        insertidentity_id = NULL;
    ELSE
        SELECT INTO insertidentity_id id
               FROM insertidentity
               WHERE insert_identity = in_insert_identity;
        IF NOT FOUND THEN
            INSERT INTO insertidentity (insert_identity)
                   VALUES (in_insert_identity) RETURNING id INTO insertidentity_id;
        END IF;
    END IF;

    INSERT INTO usagedata (
                        record_id,
                        create_time,
                        global_user_name_id,
                        vo_information_id,
                        machine_name_id,
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
                        insert_identity_id,
                        insert_time
                    )
            VALUES (
                        in_record_id,
                        in_create_time,
                        globalusername_id,
                        voinformation_id,
                        machinename_id,
                        in_global_job_id,
                        in_local_job_id,
                        in_local_user_id,
                        in_job_name,
                        in_charge,
                        in_status,
                        in_queue,
                        in_host,
                        in_node_count,
                        in_project_name,
                        in_submit_host,
                        in_start_time,
                        in_end_time,
                        in_submit_time,
                        in_cpu_duration,
                        in_wall_duration,
                        in_ksi2k_cpu_duration,
                        in_ksi2k_wall_duration,
                        in_user_time,
                        in_kernel_time,
                        in_major_page_faults,
                        in_runtime_environments,
                        in_exit_code,
                        in_insert_hostname,
                        insertidentity_id,
                        in_insert_time
                    )
            RETURNING id into usagerecord_id;

    -- finally we update the table describing what aggregated information should be updated
    PERFORM * FROM uraggregated_update WHERE insert_time = in_insert_time::date AND host = in_machine_name;
    IF NOT FOUND THEN
        INSERT INTO uraggregated_update (insert_time, host) VALUES (in_insert_time, in_machine_name);
    END IF;

    result[0] = in_record_id;
    result[1] = usagerecord_id;
    RETURN result;

END;
$recordid_rowid$
LANGUAGE plpgsql;



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

