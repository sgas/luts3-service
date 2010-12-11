-- SGAS PostgreSQL functions

CREATE OR REPLACE FUNCTION urcreate (
    in_record_id               varchar,
    in_create_time             timestamp,
    in_global_job_id           varchar,
    in_local_job_id            varchar,
    in_local_user_id           varchar,
    in_global_user_name        varchar,
    in_vo_type                 varchar,
    in_vo_issuer               varchar,
    in_vo_name                 varchar,
    in_vo_attributes           varchar[][],
    in_machine_name            varchar,
    in_job_name                varchar,
    in_charge                  numeric,
    in_status                  varchar,
    in_queue                   varchar,
    in_host                    varchar,
    in_node_count              smallint,
    in_processors              smallint,
    in_project_name            varchar,
    in_submit_host             varchar,
    in_start_time              timestamp,
    in_end_time                timestamp,
    in_submit_time             timestamp,
    in_cpu_duration            numeric,
    in_wall_duration           numeric,
    in_user_time               integer,
    in_kernel_time             integer,
    in_major_page_faults       integer,
    in_runtime_environments    varchar[],
    in_exit_code               smallint,
    in_downloads               varchar[],
    in_uploads                 varchar[],
    in_insert_hostname         varchar,
    in_insert_identity         varchar,
    in_insert_time             timestamp
)
RETURNS varchar[] AS $recordid_rowid$

DECLARE
    globalusername_id       integer;
    voinformation_id        integer;
    machinename_id          integer;
    insertidentity_id       integer;
    jobtransferurl_id       integer;

    ur_id                   integer;
    ur_global_job_id        varchar;
    ur_machine_name         varchar;
    ur_insert_time          date;

    result                  varchar[];
BEGIN
    -- first check that we do not have the record already
    SELECT usagedata.id, global_job_id, machine_name, insert_time::date
           INTO ur_id, ur_global_job_id, ur_machine_name, ur_insert_time
           FROM usagedata
               LEFT OUTER JOIN machinename ON (usagedata.machine_name_id = machinename.id)
           WHERE record_id = in_record_id;
    IF FOUND THEN
        -- this will decide if a record should replace another:
        -- if the global_job_id of the new record is similar to the global_job_id record
        -- it is considered identical. Furthermore if the global_job_id and the record_id
        -- of the incoming record are identical, the record is considered to have minimal
        -- information, and will not replace the existing record
        --
        -- this means that if the incoming record has global_job_id different from the existing
        -- record (they have the same record_id) and its global_job_id is different from the
        -- record_id, the new record will replace the existing record (the ELSE block)
        IF in_global_job_id = ur_global_job_id OR in_global_job_id = in_record_id THEN
            result[0] = in_record_id;
            result[1] = ur_id;
            RETURN result;
        ELSE
            -- delete record, mark update, and continue as normal
            DELETE from usagedata WHERE record_id = in_record_id;
            INSERT INTO uraggregated_update (insert_time, machine_name) VALUES (ur_insert_time, ur_machine_name);
        END IF;
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
                        processors,
                        project_name,
                        submit_host,
                        start_time,
                        end_time,
                        submit_time,
                        cpu_duration,
                        wall_duration,
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
                        in_processors,
                        in_project_name,
                        in_submit_host,
                        in_start_time,
                        in_end_time,
                        in_submit_time,
                        in_cpu_duration,
                        in_wall_duration,
                        in_user_time,
                        in_kernel_time,
                        in_major_page_faults,
                        in_runtime_environments,
                        in_exit_code,
                        in_insert_hostname,
                        insertidentity_id,
                        in_insert_time
                    )
            RETURNING id into ur_id;

    -- create rows for file transfers
    IF in_downloads IS NOT NULL THEN
        FOR i IN array_lower(in_downloads, 1) .. array_upper(in_downloads, 1) LOOP
            -- check if url exists, insert if it does not
            SELECT INTO jobtransferurl_id id FROM jobtransferurl WHERE url = in_downloads[i][1];
            IF NOT FOUND THEN
                INSERT INTO jobtransferurl (url) VALUES (in_downloads[i][1]) RETURNING id INTO jobtransferurl_id;
            END IF;
            -- insert download
            INSERT INTO jobtransferdata (usage_data_id, job_transfer_url_id, transfer_type,
                                         size, start_time, end_time, bypass_cache, retrieved_from_cache)
                   VALUES (ur_id, jobtransferurl_id, 'download',
                           in_downloads[i][2]::integer, in_downloads[i][3]::timestamp, in_downloads[i][4]::timestamp,
                           in_downloads[i][5]::boolean, in_downloads[i][6]::boolean);
        END LOOP;
    END IF;

    IF in_uploads IS NOT NULL THEN
        FOR i IN array_lower(in_uploads, 1) .. array_upper(in_uploads, 1) LOOP
            -- check if url exists, insert if it does not
            SELECT INTO jobtransferurl_id id FROM jobtransferurl WHERE url = in_uploads[i][1];
            IF NOT FOUND THEN
                INSERT INTO jobtransferurl (url) VALUES (in_uploads[i][1]) RETURNING id INTO jobtransferurl_id;
            END IF;
            -- insert upload
            INSERT INTO jobtransferdata (usage_data_id, job_transfer_url_id, transfer_type, size, start_time, end_time)
                   VALUES (ur_id, jobtransferurl_id, 'upload',
                           in_uploads[i][2]::integer, in_uploads[i][3]::timestamp, in_uploads[i][4]::timestamp);
        END LOOP;
    END IF;

    -- finally we update the table describing what aggregated information should be updated
    PERFORM * FROM uraggregated_update WHERE insert_time = in_insert_time::date AND machine_name = in_machine_name;
    IF NOT FOUND THEN
        INSERT INTO uraggregated_update (insert_time, machine_name) VALUES (in_insert_time, in_machine_name);
    END IF;

    result[0] = in_record_id;
    result[1] = ur_id;
    RETURN result;

END;
$recordid_rowid$
LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION update_uraggregate ( )
RETURNS varchar[] AS $insertdate_machinename$

DECLARE
    q_insert_date   date;
    q_machine_name  varchar;
    result          varchar[];
BEGIN
    -- the transaction isolation level for this function should be set to serializable
    -- unfortunately it is not possible to set in the function as a function always
    -- start a transaction, at which time it is to late to set it
    -- therefore we trust the caller of the function to set it for us

    -- get data for what to update
    SELECT insert_time, machine_name INTO q_insert_date, q_machine_name
        FROM uraggregated_update ORDER BY insert_time LIMIT 1;
    IF NOT FOUND THEN
        -- nothing to update
        RETURN result;
    END IF;

    -- delete aggregation update row
    DELETE FROM uraggregated_update WHERE insert_time = q_insert_date AND machine_name = q_machine_name;
    -- delete existing aggregated rows that will be updated
    DELETE FROM uraggregated WHERE insert_time = q_insert_date AND machine_name = q_machine_name;

    INSERT INTO uraggregated
    SELECT
        COALESCE(end_time::DATE, create_time::DATE)                      AS s_execute_time,
        insert_time::DATE                                                AS s_insert_time,
        machine_name                                                     AS s_machine_name,
        COALESCE(global_user_name, machine_name || ':' || local_user_id) AS s_user_identity,
        CASE WHEN vo_issuer LIKE 'file:///%' THEN NULL
             WHEN vo_issuer LIKE 'http://%'  THEN NULL
             WHEN vo_issuer LIKE 'https://%' THEN NULL
            ELSE vo_issuer
        END                                                              AS s_vo_issuer,
        CASE WHEN vo_name is NULL
            THEN COALESCE(machine_name || ':' || project_name)
            ELSE
                CASE WHEN vo_name LIKE '/%%'
                    THEN NULL
                    ELSE vo_name
                END
        END                                                              AS s_vo_name,
        vo_attributes[1][1]                                              AS s_vo_group,
        vo_attributes[1][2]                                              AS s_vo_role,
        count(*)                                                         AS s_n_jobs,
        SUM(COALESCE(cpu_duration,0))  / 3600.0                          AS s_cputime,
        SUM(COALESCE(wall_duration,0) * COALESCE(processors,1)) / 3600.0 AS s_walltime,
        now()                                                            AS s_generate_time
    FROM
        usagerecords
    WHERE
        insert_time::date = q_insert_date AND machine_name = q_machine_name
    GROUP BY
        s_execute_time, s_insert_time, s_machine_name, s_user_identity, s_vo_issuer, s_vo_name, s_vo_group, s_vo_role;

    result[0] = q_insert_date::varchar;
    result[1] = q_machine_name;
    RETURN result;

END;
$insertdate_machinename$
LANGUAGE plpgsql;



