-- logic for upgrading the SGAS PostgreSQL schema from version 3.2 to 3.3
-- SGAS should be stopped when performing this upgrade


-- usagedata

-- node count have previously been used for counting processors
ALTER TABLE usagedata ADD COLUMN processors INTEGER;
-- move data
UPDATE usagedata SET processors = node_count;
UPDATE usagedata SET node_count = NULL;


-- drop urcreate function

DROP FUNCTION urcreate (varchar, timestamp without time zone, varchar, varchar, varchar, varchar,
                        varchar, varchar, varchar, varchar[], varchar, varchar, numeric, varchar,
                        varchar, varchar, integer, varchar, varchar, timestamp, timestamp,timestamp,
                        numeric, numeric, numeric, numeric, integer, integer, integer, varchar[],
                        integer, varchar, varchar, timestamp);

-- new urcreate function should come from loading the function file (done seperately)

-- create view with processors column

DROP VIEW usagerecords;

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
    processors,
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



-- aggregation

ALTER TABLE uraggregated ALTER COLUMN cputime TYPE numeric(14,2);
ALTER TABLE uraggregated ALTER COLUMN walltime TYPE numeric(14,2);

-- trigger re-aggregation of all data
TRUNCATE TABLE uraggregated;
TRUNCATE TABLE uraggregated_update;
INSERT INTO uraggregated_update SELECT DISTINCT insert_time::DATE, machine_name FROM usagerecords;

