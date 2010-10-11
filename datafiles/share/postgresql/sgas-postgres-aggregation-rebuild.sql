-- Rebuild SGAS aggregation table
-- Running this script can often take a bit of time - often 5-10 minutes
-- Increasing the work_mem parameter to 16-24 MB (from the default 1 MB),
-- can decrease the time significantly.
-- SGAS should NOT be running while this script is running.


-- first clear the aggregation tables
TRUNCATE TABLE uraggregated;
TRUNCATE TABLE uraggregated_update;

-- rebuild everything with a single statement
INSERT INTO uraggregated
SELECT
    COALESCE(end_time::DATE, create_time::DATE)                      AS s_execute_time,
    insert_time::DATE                                                AS s_insert_time,
    machine_name                                                     AS s_machine_name,
    COALESCE(global_user_name, machine_name || ':' || local_user_id) AS s_user_identity,
    CASE WHEN vo_issuer LIKE 'file:///%%'
        THEN NULL
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
GROUP BY
    s_execute_time, s_insert_time, s_machine_name, s_user_identity, s_vo_issuer, s_vo_name, s_vo_group, s_vo_role
ORDER BY
    s_execute_time, s_insert_time, s_machine_name, s_user_identity, s_vo_issuer, s_vo_name, s_vo_group, s_vo_role;
