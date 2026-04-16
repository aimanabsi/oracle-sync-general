-- Enable LogMiner for Oracle CDC on Hub Server
-- Run as SYSDBA

-- Set recovery area size and location
ALTER SYSTEM SET db_recovery_file_dest_size = 20G;
ALTER SYSTEM SET db_recovery_file_dest = '/u01/app/oracle/oradata/recovery_area' SCOPE=SPFILE;

-- Create LogMiner tablespace on CDB
CREATE TABLESPACE LOGMINER_TBS DATAFILE
  '/u01/app/oracle/oradata/ORCLCDB/logminer_tbs.dbf' SIZE 25M 
  REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;

-- Create LogMiner tablespace on PDB
CREATE TABLESPACE LOGMINER_TBS DATAFILE
  '/u01/app/oracle/oradata/ORCLCDB/orclpdb/logminer_tbs.dbf' SIZE 25M 
  REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;

-- Enable archive mode
ALTER SYSTEM SET log_archive_dest_1='LOCATION=/u01/app/oracle/oradata/recovery_area VALID_FOR=(ALL_LOGFILES,ALL_ROLES) DB_UNIQUE_NAME=ORCLCDB' SCOPE=SPFILE;

-- Enable supplemental logging for all columns
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

-- Enable supplemental logging for primary key columns
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;

-- Enable supplemental logging for unique key columns
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (UNIQUE KEY) COLUMNS;

-- Enable supplemental logging for foreign key columns
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (FOREIGN KEY) COLUMNS;

-- Enable supplemental logging for all columns
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- Restart database to apply changes
SHUTDOWN IMMEDIATE;
STARTUP;

-- Verify LogMiner is enabled
SELECT SUPPLEMENTAL_LOG_DATA_MIN, SUPPLEMENTAL_LOG_DATA_PK, 
       SUPPLEMENTAL_LOG_DATA_UI, SUPPLEMENTAL_LOG_DATA_FK, 
       SUPPLEMENTAL_LOG_DATA_ALL
FROM V$DATABASE;

-- Enable archive log mode
ALTER DATABASE ARCHIVELOG;

-- Verify archive log mode
ARCHIVE LOG LIST;
