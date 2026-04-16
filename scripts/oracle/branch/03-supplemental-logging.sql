-- Enable Supplemental Logging for Z12026 and ZLAB Schemas on Branch Server
-- Run as SYSDBA on PDB

ALTER SESSION SET CONTAINER=ORCLPDB;

-- Enable database-level supplemental logging
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

-- Enable supplemental logging for primary keys
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;

-- Enable supplemental logging for unique keys
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (UNIQUE KEY) COLUMNS;

-- Enable supplemental logging for foreign keys
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (FOREIGN KEY) COLUMNS;

-- Enable supplemental logging for all columns
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- Enable supplemental logging for specific tables in Z12026 schema
-- This ensures all columns are logged for CDC
BEGIN
  FOR tab IN (SELECT table_name FROM dba_tables WHERE owner=\'Z12026\')
  LOOP
    EXECUTE IMMEDIATE \'ALTER TABLE Z12026.\' || tab.table_name || \' ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS\';
  END LOOP;
END;
/

-- Enable supplemental logging for specific tables in ZLAB schema
BEGIN
  FOR tab IN (SELECT table_name FROM dba_tables WHERE owner=\'ZLAB\')
  LOOP
    EXECUTE IMMEDIATE \'ALTER TABLE ZLAB.\' || tab.table_name || \' ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS\';
  END LOOP;
END;
/

-- Verify supplemental logging is enabled
SELECT SUPPLEMENTAL_LOG_DATA_MIN, SUPPLEMENTAL_LOG_DATA_PK, 
       SUPPLEMENTAL_LOG_DATA_UI, SUPPLEMENTAL_LOG_DATA_FK, 
       SUPPLEMENTAL_LOG_DATA_ALL
FROM V$DATABASE;

-- List all tables with supplemental logging enabled
SELECT owner, table_name, log_group_type, always
FROM dba_log_groups
WHERE owner IN (\'Z12026\', \'ZLAB\')
ORDER BY owner, table_name;
