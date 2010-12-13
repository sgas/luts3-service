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

-- now the actual changes
ALTER table usagedata DROP COLUMN ksi2k_cpu_duration;
ALTER table usagedata DROP COLUMN ksi2k_wall_duration;

ALTER TABLE usagedata ALTER COLUMN exit_code TYPE smallint;
ALTER TABLE usagedata ALTER COLUMN node_count TYPE smallint;
ALTER TABLE usagedata ALTER COLUMN processors TYPE smallint;


CREATE TABLE runtimeenvironment (
    id                      serial          NOT NULL PRIMARY KEY,
    runtime_environment     varchar(512)    NOT NULL UNIQUE
);

CREATE TABLE runtimeenvironment_usagedata (
    usagedata_id            integer         NOT NULL REFERENCES usagedata (id),
    runtimeenvironments_id  integer         NOT NULL REFERENCES runtimeenvironment(id),
    PRIMARY KEY (usagedata_id, runtimeenvironments_id)
);

-- populate runtime environments table
INSERT INTO runtimeenvironment (runtime_environment)
    SELECT DISTINCT unnest(runtime_environments) FROM usagedata;

-- populate runtimeenvironment_usagedata table
INSERT INTO runtimeenvironment_usagedata (usagedata_id, runtimeenvironments_id)
SELECT idre.id, runtimeenvironment.id FROM
    (SELECT usagedata.id, unnest(usagedata.runtime_environments) AS re FROM usagedata) AS idre
    LEFT OUTER JOIN runtimeenvironment ON (idre.re = runtimeenvironment.runtime_environment) ORDER BY idre.id;

-- drop column with runtime environments
ALTER TABLE usagedata DROP COLUMN runtime_environments;


SELECT 'View and functions dropped, you should reload them' AS Message;


