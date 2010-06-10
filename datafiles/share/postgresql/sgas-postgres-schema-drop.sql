-- SGAS PostgreSQL schema
-- drop statement to clear the database

DROP VIEW usagerecords;

DROP TABLE usagedata;
DROP TABLE insertidentity;
DROP TABLE machinename;
DROP TABLE voinformation;
DROP TABLE globalusername;

DROP TABLE uraggregated;
DROP TABLE uraggregated_update;

DROP FUNCTION urcreate ( character varying, timestamp without time zone, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying[], character varying, character varying, numeric, character varying, character varying, character varying, integer, character varying, character varying, timestamp without time zone, timestamp without time zone, timestamp without time zone, numeric, numeric, numeric, numeric, integer, integer, integer, character varying[], integer, character varying, character varying, timestamp without time zone) ;


