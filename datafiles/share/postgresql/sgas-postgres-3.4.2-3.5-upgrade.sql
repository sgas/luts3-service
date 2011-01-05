-- logic for upgrading the SGAS PostgreSQL schema from version 3.4 to 3.5
-- SGAS should be stopped when performing this upgrade


-- drop old view and functions so we can change stuff
DROP VIEW usagerecords;

DROP FUNCTION urcreate (character varying,
                        timestamp without time zone,
                        character varying,
                        character varying,
                        character varying,
                        character varying,
                        character varying,
                        character varying,
                        character varying,
                        character varying[],
                        character varying,
                        character varying,
                        numeric,
                        character varying,
                        character varying,
                        character varying,
                        integer,
                        integer,
                        character varying,
                        character varying,
                        timestamp without time zone,
                        timestamp without time zone,
                        timestamp without time zone,
                        numeric,
                        numeric,
                        numeric,
                        numeric,
                        integer,
                        integer,
                        integer,
                        character varying[],
                        integer,
                        character varying[],
                        character varying[],
                        character varying,
                        character varying,
                        timestamp without time zone
);


-- create new tables for normalizing
CREATE TABLE runtimeenvironment (
    id                      serial          NOT NULL PRIMARY KEY,
    runtime_environment     varchar(512)    NOT NULL UNIQUE
);

CREATE TABLE runtimeenvironment_usagedata (
    usagedata_id            integer         NOT NULL REFERENCES usagedata (id),
    runtimeenvironments_id  integer         NOT NULL REFERENCES runtimeenvironment(id),
    PRIMARY KEY (usagedata_id, runtimeenvironments_id)
);

CREATE TABLE inserthost (
    id                      serial          NOT NULL PRIMARY KEY,
    insert_host             varchar(1024)   NOT NULL UNIQUE
);

CREATE TABLE jobstatus (
    id                      serial          NOT NULL PRIMARY KEY,
    status                  varchar(100)    NOT NULL UNIQUE
);

CREATE TABLE jobqueue (
    id                      serial          NOT NULL PRIMARY KEY,
    queue                   varchar(200)    NOT NULL UNIQUE
);

CREATE TABLE hostscalefactors (
    machine_name            varchar(200)    NOT NULL UNIQUE PRIMARY KEY,
    scale_factor            float           NOT NULL
);

-- populate new tables
INSERT INTO runtimeenvironment (runtime_environment) SELECT DISTINCT unnest(runtime_environments) FROM usagedata;

INSERT INTO runtimeenvironment_usagedata (usagedata_id, runtimeenvironments_id)
    SELECT idre.id, runtimeenvironment.id FROM
        (SELECT usagedata.id, unnest(usagedata.runtime_environments) AS re FROM usagedata) AS idre
        LEFT OUTER JOIN runtimeenvironment ON (idre.re = runtimeenvironment.runtime_environment) ORDER BY idre.id;

INSERT INTO inserthost (insert_host) SELECT DISTINCT insert_hostname FROM usagedata WHERE insert_hostname IS NOT NULL;

INSERT INTO jobstatus (status) SELECT DISTINCT status FROM usagedata WHERE status IS NOT NULL;

INSERT INTO jobqueue (queue) SELECT DISTINCT queue FROM usagedata WHERE queue IS NOT NULL;

-- renames does not require table scans so we don't have to batch them
ALTER TABLE usagedata RENAME COLUMN insert_hostname TO insert_host_id;
ALTER TABLE usagedata RENAME COLUMN status TO status_id;
ALTER TABLE usagedata RENAME COLUMN queue TO queue_id;

-- update previous not-normalized values to id/foreign keys
UPDATE usagedata SET insert_host_id = (SELECT id FROM inserthost WHERE insert_host = insert_host_id),
                     status_id = (SELECT id FROM jobstatus WHERE jobstatus.status = usagedata.status_id),
                     queue_id = (SELECT id FROM jobqueue WHERE jobqueue.queue = usagedata.queue_id);

-- now the actual changes
ALTER TABLE usagedata
    DROP COLUMN ksi2k_cpu_duration,
    DROP COLUMN ksi2k_wall_duration,
    DROP COLUMN runtime_environments,
    ALTER COLUMN exit_code TYPE smallint,
    ALTER COLUMN node_count TYPE smallint,
    ALTER COLUMN processors TYPE smallint,
    ALTER COLUMN insert_host_id TYPE integer USING CAST(insert_host_id AS integer),
    ALTER COLUMN status_id TYPE integer USING CAST(status_id AS integer),
    ALTER COLUMN queue_id TYPE integer USING CAST(queue_id AS integer),
    ADD CONSTRAINT usagedata_insert_host_id_fkey FOREIGN KEY (insert_host_id) REFERENCES inserthost (id),
    ADD CONSTRAINT usagedata_status_id_fkey FOREIGN KEY (status_id) REFERENCES jobstatus (id),
    ADD CONSTRAINT usagedata_queue_id_fkey FOREIGN KEY (queue_id) REFERENCES jobqueue (id);


-- new aggregation schema
DROP FUNCTION update_uraggregate();

DROP TABLE uraggregated;
DROP TABLE uraggregated_update;

CREATE TABLE uraggregated_data (
    execution_time          date,
    insert_time             date,
    machine_name_id         integer,
    queue_id                integer,
    global_user_name_id     integer,
    local_user_id           varchar(500),
    vo_information_id       integer,
    project_name            varchar(200),
    runtime_environments_id integer[],
    status_id               integer,
    n_jobs                  integer,
    cputime                 numeric(14,2),
    walltime                numeric(14,2),
    generate_time           timestamp
);

CREATE TABLE uraggregated_update (
    insert_time         date,
    machine_name_id     integer
);

INSERT INTO uraggregated_update SELECT DISTINCT insert_time::DATE, machine_name_id FROM usagedata;


SELECT 'View and functions dropped, you should reload them' AS Message;


