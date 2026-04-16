-- Create Debezium Sync User on Hub Server
-- Run as SYSDBA

-- Connect to PDB
ALTER SESSION SET CONTAINER=ORCLPDB;

-- Create sync user
CREATE USER debezium IDENTIFIED BY debezium_password;

-- Grant necessary privileges to debezium user
GRANT CREATE SESSION TO debezium;
GRANT CONNECT TO debezium;
GRANT RESOURCE TO debezium;

-- Grant SELECT on all tables in Z12026 schema
GRANT SELECT ON Z12026.* TO debezium;

-- Grant SELECT on all tables in ZLAB schema
GRANT SELECT ON ZLAB.* TO debezium;

-- Grant LogMiner privileges
GRANT EXECUTE ON DBMS_LOGMNR TO debezium;
GRANT EXECUTE ON DBMS_LOGMNR_D TO debezium;
GRANT SELECT ON V_$LOG TO debezium;
GRANT SELECT ON V_$LOGFILE TO debezium;
GRANT SELECT ON V_$ARCHIVED_LOG TO debezium;
GRANT SELECT ON V_$ARCHIVE_DEST TO debezium;
GRANT SELECT ON V_$DATABASE TO debezium;
GRANT SELECT ON V_$THREAD TO debezium;
GRANT SELECT ON V_$PARAMETER TO debezium;
GRANT SELECT ON V_$NLS_PARAMETERS TO debezium;
GRANT SELECT ON V_$TIMEZONE_NAMES TO debezium;
GRANT SELECT ON V_$TRANSACTION TO debezium;
GRANT SELECT ON V_$ROLLNAME TO debezium;
GRANT SELECT ON V_$INSTANCE TO debezium;
GRANT SELECT ON V_$LOG_HISTORY TO debezium;

-- Grant privileges on DBA views
GRANT SELECT ON DBA_OBJECTS TO debezium;
GRANT SELECT ON DBA_TABLES TO debezium;
GRANT SELECT ON DBA_TAB_COLUMNS TO debezium;
GRANT SELECT ON DBA_CONSTRAINTS TO debezium;
GRANT SELECT ON DBA_CONS_COLUMNS TO debezium;
GRANT SELECT ON DBA_INDEXES TO debezium;
GRANT SELECT ON DBA_IND_COLUMNS TO debezium;
GRANT SELECT ON DBA_SEQUENCES TO debezium;
GRANT SELECT ON DBA_SYNONYMS TO debezium;
GRANT SELECT ON DBA_VIEWS TO debezium;
GRANT SELECT ON DBA_TAB_PRIVS TO debezium;
GRANT SELECT ON DBA_ROLE_PRIVS TO debezium;
GRANT SELECT ON DBA_USERS TO debezium;
GRANT SELECT ON DBA_SEGMENTS TO debezium;
GRANT SELECT ON DBA_EXTENTS TO debezium;

-- Grant privileges for supplemental logging
GRANT ALTER SYSTEM TO debezium;
GRANT ALTER DATABASE TO debezium;

-- Create role for sync operations
CREATE ROLE sync_role;
GRANT CONNECT TO sync_role;
GRANT SELECT ON Z12026.* TO sync_role;
GRANT SELECT ON ZLAB.* TO sync_role;
GRANT sync_role TO debezium;

-- Verify user creation
SELECT username, account_status FROM dba_users WHERE username='DEBEZIUM';
