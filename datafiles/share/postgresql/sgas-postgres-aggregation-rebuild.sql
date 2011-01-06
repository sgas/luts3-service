-- Rebuild SGAS aggregation table
-- Running this script can often take a bit of time - often 5-10 minutes
-- Increasing the work_mem parameter to 16-24 MB (from the default 1 MB),
-- can decrease the time significantly.
-- SGAS should NOT be running while this script is running.


-- clear the aggregation tables
TRUNCATE TABLE uraggregated_data;
TRUNCATE TABLE uraggregated_update;

-- update all aggregation combinations
INSERT INTO uraggregated_update SELECT DISTINCT insert_time::DATE, machine_name_id FROM usagedata;

-- function for performing aggregation steps until all done
CREATE FUNCTION uraggregated_upate_all() RETURNS integer AS $updates$
DECLARE
    updates     integer;
    update_info varchar[];
BEGIN
    updates = 0;
    -- call update_uraggregate until nothing more to do
    LOOP
        SELECT update_uraggregate() INTO update_info;
        IF update_info IS NULL THEN
            EXIT;
        END IF;
        updates = updates + 1;
    END LOOP;
    RETURN updates;
END;
$updates$
LANGUAGE plpgsql;

SELECT uraggregated_upate_all();

DROP FUNCTION uraggregated_upate_all();





