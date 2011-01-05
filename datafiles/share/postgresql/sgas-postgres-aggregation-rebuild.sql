-- Rebuild SGAS aggregation table
-- Running this script can often take a bit of time - often 5-10 minutes
-- Increasing the work_mem parameter to 16-24 MB (from the default 1 MB),
-- can decrease the time significantly.
-- SGAS should NOT be running while this script is running.


-- first clear the aggregation tables
TRUNCATE TABLE uraggregated_data;
TRUNCATE TABLE uraggregated_update;


-- rebuild everything with a single statement
INSERT INTO uraggregated_data
SELECT
    COALESCE(end_time::DATE, create_time::DATE)                     AS s_execute_time,
    insert_time::DATE                                               AS s_insert_time,
    machine_name_id                                                 AS s_machine_name_id,
    queue_id                                                        AS s_queue_id,
    global_user_name_id                                             AS s_global_user_name_id,
    CASE WHEN global_user_name_id IS NULL THEN local_user_id
         ELSE NULL
    END                                                             AS s_local_user_id,
    vo_information_id                                               AS s_vo_information_id,
    CASE WHEN vo_information_id IS NULL THEN project_name
         ELSE NULL
    END                                                             AS s_project_name,
    ARRAY(SELECT runtimeenvironment_usagedata.runtimeenvironments_id
          FROM runtimeenvironment_usagedata
          WHERE usagedata.id = runtimeenvironment_usagedata.usagedata_id) AS s_runtime_environments,
    status_id                                                        AS s_status_id,
    count(*)                                                         AS s_n_jobs,
    SUM(COALESCE(cpu_duration,0))                                    AS s_cputime,
    SUM(COALESCE(wall_duration,0) * COALESCE(processors,1))          AS s_walltime,
    now()                                                            AS s_generate_time
FROM
    usagedata
GROUP BY
    s_execute_time, s_insert_time, s_machine_name_id, s_queue_id,
    s_global_user_name_id, s_local_user_id, s_vo_information_id,
    s_project_name, s_runtime_environments, s_status_id
ORDER BY
    s_execute_time, s_insert_time, s_machine_name_id, s_queue_id,
    s_global_user_name_id, s_local_user_id, s_vo_information_id,
    s_project_name, s_runtime_environments, s_status_id;

