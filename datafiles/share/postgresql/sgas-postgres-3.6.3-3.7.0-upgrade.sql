-- logic for upgrading the SGAS PostgreSQL schema from version 3.6.3 to 3.7.0
-- SGAS should be stopped when performing this upgrade

BEGIN;

CREATE TABLE site (
	    id                      serial          NOT NULL UNIQUE PRIMARY KEY,
	    site                    varchar(1000)   NOT NULL
);

-- Drop view
DROP VIEW storagerecords;

-- Create new schema
CREATE TABLE storagedata_tmp (
	    id                      serial          NOT NULL UNIQUE PRIMARY KEY,
	    record_id               varchar(1000)   NOT NULL UNIQUE,
	    create_time             timestamp       NOT NULL,
	    storage_system_id       integer         NOT NULL REFERENCES storagesystem(id),
	    storage_share_id        integer         REFERENCES storageshare(id),
	    storage_media_id        integer         REFERENCES storagemedia(id),
	    storage_class_id        integer         REFERENCES storageclass(id),
	    file_count              integer,
	    directory_path_id       integer         REFERENCES directorypath(id),
	    local_user_id           integer         REFERENCES localuser(id),
	    local_group_id          integer         REFERENCES localgroup(id),
	    user_identity_id        integer         REFERENCES useridentity(id),
	    group_identity_id       integer         REFERENCES groupidentity(id),
	    site_id                 integer         REFERENCES site(id),
	    start_time              timestamp       NOT NULL,
	    end_time                timestamp       NOT NULL,
	    resource_capacity_used  bigint          NOT NULL,
	    logical_capacity_used   bigint,
	    insert_host_id          integer         REFERENCES inserthost (id),
	    insert_identity_id      integer         REFERENCES insertidentity (id),
	    insert_time             timestamp
);

-- Fill new table.
insert into storagedata_tmp 
	select id,record_id,create_time,storage_system_id,storage_share_id,storage_media_id,
		storage_class_id,file_count,directory_path_id,local_user_id,local_group_id,
		user_identity_id,group_identity_id, NULL as site_id, 
		measure_time - valid_duration * '1s'::interval as start_time, 
		measure_time as end_time,resource_capacity_used,logical_capacity_used,
		insert_host_id,insert_identity_id,insert_time from storagedata;

-- Rename and remove old table.
ALTER TABLE storagedata RENAME TO storagedata_old;
ALTER TABLE storagedata_tmp RENAME TO storagedata;
DROP TABLE storagedata_old;

-- Create view
CREATE VIEW storagerecords AS
SELECT
        record_id                       AS record_id,
        create_time                     AS create_time,
        storagesystem.storage_system    AS storage_system,
        storageshare.storage_share      AS storage_share,
        storagemedia.storage_media      AS storage_media,
        storageclass.storage_class      AS storage_class,
        file_count                      AS file_count,
        directorypath.directory_path    AS directory_path,
        localuser.local_user            AS local_user,
        localgroup.local_group          AS local_group,
        useridentity.user_identity      AS user_identity,
        groupidentity.group_identity    AS group_identity,
        groupidentity.group_attribute   AS group_attribute,
        site.site                       AS site,
        start_time                      AS start_time,
        end_time                        AS end_time,
        resource_capacity_used          AS resource_capacity_used,
        logical_capacity_used           AS logical_capacity_used,
        inserthost.insert_host          AS insert_host,
        insertidentity.insert_identity  AS insert_identity,
        insert_time                     AS insert_time
FROM
    storagedata
LEFT OUTER JOIN storagesystem   ON (storagedata.storage_system_id   = storagesystem.id)
LEFT OUTER JOIN storageshare    ON (storagedata.storage_share_id    = storageshare.id)
LEFT OUTER JOIN storagemedia    ON (storagedata.storage_media_id    = storagemedia.id)
LEFT OUTER JOIN storageclass    ON (storagedata.storage_class_id    = storageclass.id)
LEFT OUTER JOIN directorypath   ON (storagedata.directory_path_id   = directorypath.id)
LEFT OUTER JOIN localuser       ON (storagedata.local_user_id       = localuser.id)
LEFT OUTER JOIN localgroup      ON (storagedata.local_group_id      = localgroup.id)
LEFT OUTER JOIN useridentity    ON (storagedata.user_identity_id    = useridentity.id)
LEFT OUTER JOIN groupidentity   ON (storagedata.group_identity_id   = groupidentity.id)
LEFT OUTER JOIN inserthost      ON (storagedata.insert_host_id      = inserthost.id)
LEFT OUTER JOIN insertidentity  ON (storagedata.insert_identity_id  = insertidentity.id)
LEFT OUTER JOIN site  ON (storagedata.site_id  = site.id)
;

COMMIT;

-- End of file
