-- logic for upgrading the SGAS PostgreSQL schema from version 3.6.0 to 3.6.1
-- SGAS should be stopped when performing this upgrade


-- Change foreign key to to match data it actually references
ALTER TABLE storagedata DROP CONSTRAINT storagedata_storage_class_id_fkey;
ALTER TABLE storagedata ADD  CONSTRAINT storagedata_storage_class_id_fkey FOREIGN KEY (storage_class_id) REFERENCES storageclass(id);

