-- Add Conflict Resolution Metadata Columns to Branch Server Tables
-- Run as SYSDBA on PDB

ALTER SESSION SET CONTAINER=ORCLPDB;

-- Add metadata columns to all tables in Z12026 schema for conflict tracking
BEGIN
  FOR tab IN (SELECT table_name FROM dba_tables WHERE owner='Z12026')
  LOOP
    BEGIN
      EXECUTE IMMEDIATE 'ALTER TABLE Z12026.' || tab.table_name || 
        ' ADD (_sync_scn NUMBER, _sync_timestamp TIMESTAMP, _sync_source VARCHAR2(50), _sync_branch VARCHAR2(50))';
      DBMS_OUTPUT.PUT_LINE('Added columns to Z12026.' || tab.table_name);
    EXCEPTION
      WHEN OTHERS THEN
        IF SQLCODE != -1430 THEN  -- Column already exists
          DBMS_OUTPUT.PUT_LINE('Error on Z12026.' || tab.table_name || ': ' || SQLERRM);
        END IF;
    END;
  END LOOP;
END;
/

-- Add metadata columns to all tables in ZLAB schema for conflict tracking
BEGIN
  FOR tab IN (SELECT table_name FROM dba_tables WHERE owner='ZLAB')
  LOOP
    BEGIN
      EXECUTE IMMEDIATE 'ALTER TABLE ZLAB.' || tab.table_name || 
        ' ADD (_sync_scn NUMBER, _sync_timestamp TIMESTAMP, _sync_source VARCHAR2(50), _sync_branch VARCHAR2(50))';
      DBMS_OUTPUT.PUT_LINE('Added columns to ZLAB.' || tab.table_name);
    EXCEPTION
      WHEN OTHERS THEN
        IF SQLCODE != -1430 THEN  -- Column already exists
          DBMS_OUTPUT.PUT_LINE('Error on ZLAB.' || tab.table_name || ': ' || SQLERRM);
        END IF;
    END;
  END LOOP;
END;
/

-- Create indexes on metadata columns for faster lookups
BEGIN
  FOR tab IN (SELECT table_name FROM dba_tables WHERE owner='Z12026')
  LOOP
    BEGIN
      EXECUTE IMMEDIATE 'CREATE INDEX Z12026.' || tab.table_name || '_sync_idx ON Z12026.' || tab.table_name || 
        '(_sync_scn, _sync_timestamp)';
      DBMS_OUTPUT.PUT_LINE('Created index on Z12026.' || tab.table_name);
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Index creation skipped for Z12026.' || tab.table_name);
    END;
  END LOOP;
END;
/

BEGIN
  FOR tab IN (SELECT table_name FROM dba_tables WHERE owner='ZLAB')
  LOOP
    BEGIN
      EXECUTE IMMEDIATE 'CREATE INDEX ZLAB.' || tab.table_name || '_sync_idx ON ZLAB.' || tab.table_name || 
        '(_sync_scn, _sync_timestamp)';
      DBMS_OUTPUT.PUT_LINE('Created index on ZLAB.' || tab.table_name);
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Index creation skipped for ZLAB.' || tab.table_name);
    END;
  END LOOP;
END;
/

-- Verify metadata columns were added
SELECT owner, table_name, column_name
FROM dba_tab_columns
WHERE owner IN ('Z12026', 'ZLAB')
AND column_name LIKE '_sync%'
ORDER BY owner, table_name, column_name;
