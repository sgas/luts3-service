
AGGREGATE_STATEMENT = """
DROP TABLE uraggregated;
-- this is the generation for the aggregation table
-- this statement should be invoked regularly to in order to update the table
CREATE TABLE uraggregated AS
SELECT
    CASE WHEN end_time IS NULL
        THEN create_time::DATE
        ELSE end_time::DATE
    END                                 AS execution_date,
    insert_time::DATE                   AS insert_date,
    machine_name,
    CASE WHEN global_user_name ISNULL
        THEN (machine_name || ':' || local_user_id)
        ELSE global_user_name
    END                                 AS user_identity,
    CASE WHEN vo_issuer LIKE 'file:///%'
        THEN NULL
        ELSE vo_issuer
    END                                 AS vo_issuer,
    CASE WHEN vo_name LIKE '/%'
        THEN NULL
        ELSE vo_name
    END                                 AS vo_name,
    vo_attributes[1][1]                 AS vo_group,
    vo_attributes[1][2]                 AS vo_role,
    count(*)                            AS n_jobs,
    SUM(COALESCE(cpu_duration,0))       AS sum_cputime,
    SUM(COALESCE(wall_duration,0))      AS sum_walltime,
    now()                               AS generate_time
FROM
    usagerecords
GROUP BY
    CASE WHEN end_time IS NULL THEN create_time::DATE ELSE end_time::DATE END,
    insert_time::DATE,
    machine_name,
    CASE WHEN global_user_name ISNULL
        THEN (machine_name || ':' || local_user_id)
        ELSE global_user_name
    END,
    vo_issuer,
    vo_name,
    vo_attributes
;
"""

