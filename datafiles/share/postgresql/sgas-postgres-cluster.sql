-- this script will reorganize the records are ordered on disk by insert date
-- SGAS can be running while the script is run, however it is recommended to stop it
-- It is also recommend to backup up the database first (before any data mingling really)


-- first drop hash index, so we don't have to update it during the clustering
DROP INDEX insert_time_date_hash_idx;

-- create b-tree index, needed for the clustering
CREATE INDEX insert_time_tree_idx on usagedata (insert_time) ;

-- cluster the data, this will take a while
CLUSTER usagedata using insert_time_tree_idx;

-- drop b-tree index
DROP INDEX insert_time_tree_idx;

-- restore hash index over insert time
CREATE INDEX insert_time_date_hash_idx ON usagedata USING HASH (date(insert_time));

