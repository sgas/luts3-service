-- upgrade sgas luts schema from version 3.5.x to 3.6.0
-- the changes are the addition of a storage record schema

CREATE TABLE storagesystem (
    id                      serial          NOT NULL UNIQUE PRIMARY KEY,
    storage_system          varchar(1000)   NOT NULL UNIQUE
);


CREATE TABLE storageshare (
    id                      serial          NOT NULL UNIQUE PRIMARY KEY,
    storage_share           varchar(1000)   NOT NULL UNIQUE
);


CREATE TABLE storagemedia (
    id                      serial          NOT NULL UNIQUE PRIMARY KEY,
    storage_media           varchar(1000)   NOT NULL UNIQUE
);


CREATE TABLE storageclass (
    id                      serial          NOT NULL UNIQUE PRIMARY KEY,
    storage_class           varchar(1000)   NOT NULL UNIQUE
);


CREATE TABLE directorypath (
    id                      serial          NOT NULL UNIQUE PRIMARY KEY,
    directory_path          varchar(2000)   NOT NULL UNIQUE
);


CREATE TABLE localgroup (
    id                      serial          NOT NULL UNIQUE PRIMARY KEY,
    local_group             varchar(1000)   NOT NULL UNIQUE
);


CREATE TABLE useridentity (
    id                      serial          NOT NULL UNIQUE PRIMARY KEY,
    user_identity           varchar(2000)   NOT NULL UNIQUE
);


-- group is a reserved word, so we use group identity
CREATE TABLE groupidentity (
    id                      serial          NOT NULL UNIQUE PRIMARY KEY,
    group_identity          varchar(1000)   NOT NULL,
    group_attribute         varchar(100)[][]
);


CREATE TABLE storagedata (
    id                      serial          NOT NULL UNIQUE PRIMARY KEY,
    record_id               varchar(1000)   NOT NULL UNIQUE,
    create_time             timestamp       NOT NULL,
    storage_system_id       integer         NOT NULL REFERENCES storagesystem(id),
    storage_share_id        integer         REFERENCES storageshare(id),
    storage_media_id        integer         REFERENCES storagemedia(id),
    storage_class_id        integer         REFERENCES storageshare(id),
    file_count              integer,
    directory_path_id       integer         REFERENCES directorypath(id),
    local_user_id           integer         REFERENCES localuser(id),
    local_group_id          integer         REFERENCES localgroup(id),
    user_identity_id        integer         REFERENCES useridentity(id),
    group_identity_id       integer         REFERENCES groupidentity(id),
    measure_time            timestamp       NOT NULL,
    valid_duration          integer,        -- seconds
    resource_capacity_used  bigint          NOT NULL,
    logical_capacity_used   bigint,
    insert_host_id          integer         REFERENCES inserthost (id),
    insert_identity_id      integer         REFERENCES insertidentity (id),
    insert_time             timestamp
);


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
        measure_time                    AS measure_time,
        valid_duration                  AS valid_duration,
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
;


CREATE FUNCTION srcreate (
    in_record_id            varchar,
    in_create_time          timestamp,
    in_storage_system       varchar,
    in_storage_share        varchar,
    in_storage_media        varchar,
    in_storage_class        varchar,
    in_file_count           integer,
    in_directory_path       varchar,
    in_local_user           varchar,
    in_local_group          varchar,
    in_user_identity        varchar,
    in_group_identity       varchar,
    in_group_attribute     varchar[][],
    in_measure_time         timestamp,
    in_valid_duration       integer,
    in_resource_capacity_used   bigint,
    in_logical_capacity_used    bigint,
    in_insert_host          varchar,
    in_insert_identity      varchar,
    in_insert_time          timestamp
)
RETURNS
    varchar[] as $recordid_rowid$
DECLARE
    storage_system_key      integer;
    storage_share_key       integer;
    storage_media_key       integer;
    storage_class_key       integer;
    directory_path_key      integer;
    local_user_key          integer;
    local_group_key         integer;
    user_identity_key       integer;
    group_identity_key      integer;
    insert_host_key         integer;
    insert_identity_key     integer;

    sr_key                  integer;
    result                  varchar[];
BEGIN
    -- first check if the record already exists
    SELECT storagedata.id INTO sr_key FROM storagedata WHERE record_id = in_record_id;
    IF FOUND THEN
        -- just return the entry from the already existing record
        result[0] = in_record_id;
        result[1] = sr_key;
        RETURN result;
    END IF;

    -- storage system
    IF in_storage_system IS NULL THEN
        RAISE EXCEPTION 'Storage system parameter must be non-null.';
    ELSE
        SELECT INTO storage_system_key id FROM storagesystem WHERE storage_system = in_storage_system;
        IF NOT FOUND THEN
            INSERT INTO storagesystem (storage_system) VALUES (in_storage_system) RETURNING id INTO storage_system_key;
        END IF;
    END IF;

    -- storage share
    IF in_storage_share IS NULL THEN
        storage_share_key = NULL;
    ELSE
        SELECT INTO storage_share_key id FROM storageshare WHERE storage_share = in_storage_share;
        IF NOT FOUND THEN
            INSERT INTO storageshare (storage_share) VALUES (in_storage_share) RETURNING id INTO storage_share_key;
        END IF;
    END IF;

    -- storage media
    IF in_storage_media IS NULL THEN
        storage_media_key = NULL;
    ELSE
        SELECT INTO storage_media_key id FROM storagemedia WHERE storage_media = in_storage_media;
        IF NOT FOUND THEN
            INSERT INTO storagemedia (storage_media) VALUES (in_storage_media) RETURNING id INTO storage_media_key;
        END IF;
    END IF;

    -- storage class
    IF in_storage_class IS NULL THEN
        storage_class_key = NULL;
    ELSE
        SELECT INTO storage_class_key id FROM storageclass WHERE storage_class = in_storage_class;
        IF NOT FOUND THEN
            INSERT INTO storageclass (storage_class) VALUES (in_storage_class) RETURNING id INTO storage_class_key;
        END IF;
    END IF;

    -- directory path
    IF in_directory_path IS NULL THEN
        directory_path_key = NULL;
    ELSE
        SELECT INTO directory_path_key id FROM directorypath WHERE directory_path = in_directory_path;
        IF NOT FOUND THEN
            INSERT INTO directorypath (directory_path) VALUES (in_directory_path) RETURNING id INTO directory_path_key;
        END IF;
    END IF;

    -- local user
    IF in_local_user IS NULL THEN
        local_user_key = NULL;
    ELSE
        SELECT INTO local_user_key id FROM localuser WHERE local_user = in_local_user;
        IF NOT FOUND THEN
            INSERT INTO localuser (local_user) VALUES (in_local_user) RETURNING id INTO local_user_key;
        END IF;
    END IF;

    -- local group
    IF in_local_group IS NULL THEN
        local_group_key = NULL;
    ELSE
        SELECT INTO local_group_key id FROM localgroup WHERE local_group = in_local_group;
        IF NOT FOUND THEN
            INSERT INTO localgroup (local_group) VALUES (in_local_group) RETURNING id INTO local_group_key;
        END IF;
    END IF;

    -- user identity
    IF in_user_identity IS NULL THEN
        user_identity_key = NULL;
    ELSE
        SELECT INTO user_identity_key id FROM useridentity WHERE user_identity = in_user_identity;
        IF NOT FOUND THEN
            INSERT INTO useridentity (user_identity) VALUES (in_user_identity) RETURNING id INTO user_identity_key;
        END IF;
    END IF;

    -- group identity
    IF in_group_identity IS NULL THEN
        group_identity_key = NULL;
    ELSE
        SELECT INTO group_identity_key id FROM groupidentity
               WHERE group_identity   IS NOT DISTINCT FROM in_group_identity AND
                     group_attribute  IS NOT DISTINCT FROM in_group_attribute;
        IF NOT FOUND THEN
            INSERT INTO groupidentity (group_identity, group_attribute) VALUES (in_group_identity, in_group_attribute) RETURNING id INTO group_identity_key;
        END IF;
    END IF;

    -- insert host
    IF in_insert_host IS NULL THEN
        insert_host_key = NULL;
    ELSE
        SELECT INTO insert_host_key id FROM inserthost WHERE insert_host = in_insert_host;
        IF NOT FOUND THEN
            INSERT INTO inserthost (insert_host) VALUES (in_insert_host) RETURNING id INTO insert_host_key;
        END IF;
    END IF;

    -- insert identity
    IF in_insert_identity IS NULL THEN
        insert_identity_key = NULL;
    ELSE
        SELECT INTO insert_identity_key id FROM insertidentity WHERE insert_identity = in_insert_identity;
        IF NOT FOUND THEN
            INSERT INTO insertidentity (insert_identity) VALUES (in_insert_identity) RETURNING id INTO insert_identity_key;
        END IF;
    END IF;

    INSERT INTO storagedata (
                    record_id,
                    create_time,
                    storage_system_id,
                    storage_share_id,
                    storage_media_id,
                    storage_class_id,
                    file_count,
                    directory_path_id,
                    local_user_id,
                    local_group_id,
                    user_identity_id,
                    group_identity_id,
                    measure_time,
                    valid_duration,
                    resource_capacity_used,
                    logical_capacity_used,
                    insert_host_id,
                    insert_identity_id,
                    insert_time
            )
            VALUES (
                    in_record_id,
                    in_create_time,
                    storage_system_key,
                    storage_share_key,
                    storage_media_key,
                    storage_class_key,
                    in_file_count,
                    directory_path_key,
                    local_user_key,
                    local_group_key,
                    user_identity_key,
                    group_identity_key,
                    in_measure_time,
                    in_valid_duration,
                    in_resource_capacity_used,
                    in_logical_capacity_used,
                    insert_host_key,
                    insert_identity_key,
                    in_insert_time
                    )
            RETURNING id INTO sr_key;

    result[0] = in_record_id;
    result[1] = sr_key;
    RETURN result;

END;
$recordid_rowid$
LANGUAGE plpgsql;

