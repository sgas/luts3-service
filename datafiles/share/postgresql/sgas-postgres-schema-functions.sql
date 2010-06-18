-- SGAS PostgreSQL functions
-- Currently just functionality for record insertion

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
    PERFORM * FROM uraggregated_update WHERE insert_time = in_insert_time::date AND machine_name = in_machine_name;
    IF NOT FOUND THEN
        INSERT INTO uraggregated_update (insert_time, machine_name) VALUES (in_insert_time, in_machine_name);
    END IF;

    result[0] = in_record_id;
    result[1] = usagerecord_id;
    RETURN result;

END;
$recordid_rowid$
LANGUAGE plpgsql;

