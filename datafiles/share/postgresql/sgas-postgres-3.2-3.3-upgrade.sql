
-- logic for upgrading the SGAS PostgreSQL schema from version 3.2 to 3.3
-- SGAS should be stopped when performing this upgrade

-- usagedata

-- node count have previously been used for counting processors
ALTER TABLE usagedata ADD COLUMN processors INTEGER USING node_count;
UPDATE usagedata SET node_count = NULL;


-- aggregation

ALTER TABLE uraggregated ALTER COLUMN cputime TYPE numeric(14,2);
ALTER TABLE uraggregated ALTER COLUMN walltime TYPE numeric(14,2);

-- trigger re-aggregation of all data
TRUNCATE TABLE uraggregated;
TRUNCATE TABLE uraggregated_update;
INSERT INTO uraggregated_update SELECT DISTINCT insert_time::DATE, machine_name FROM usagerecords;

