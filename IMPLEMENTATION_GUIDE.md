# Oracle Sync General - Bidirectional Multi-Branch Synchronization Implementation Guide

This document provides a comprehensive guide for setting up and configuring the Oracle Sync General solution, which enables bidirectional data synchronization between a central Oracle 19c database (Hub) and multiple branch servers with intermittent network connectivity. The solution leverages Kafka, Debezium Change Data Capture (CDC), MirrorMaker2, and a Spring Boot-based conflict resolution service.

## 1. Project Overview

The Oracle Sync General project addresses the challenge of maintaining data consistency across distributed Oracle databases, particularly in scenarios where branch offices may experience temporary network outages. It employs a robust hub-and-spoke architecture to ensure data integrity, conflict resolution, and high availability.

### 1.1 Key Features

*   **Bidirectional Synchronization**: Data changes are captured from both hub and branch databases and synchronized in both directions.
*   **Multi-Branch Support**: The architecture is designed to accommodate an arbitrary number of branch servers.
*   **Conflict Resolution**: Automatic detection and resolution of data conflicts using a Last-Write-Wins (LWW) strategy based on Oracle System Change Number (SCN).
*   **Offline Buffering**: Local Kafka instances on branch servers buffer data changes during network disconnections, ensuring no data loss.
*   **Schema Support**: Specifically configured to synchronize `Z12026` and `ZLAB` schemas, including all their tables.
*   **Order Preservation**: Ensures referential integrity across master-detail relationships by maintaining event ordering.
*   **Performance Optimization**: Utilizes Kafka's high-throughput capabilities and Debezium's efficient CDC mechanism.
*   **Monitoring & Dashboard**: Provides real-time insights into sync status, conflict logs, and branch connectivity through a dedicated dashboard.

### 1.2 Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│ MAIN SERVER (Hub) - Always Online                              │
│ Oracle Linux 8 + Oracle 19c                                    │
│                                                                 │
│ ┌─────────────┐ ┌──────────────┐ ┌─────────────┐ ┌──────────┐ │
│ │   Oracle    │◄─►│  Debezium   │◄─►│   Kafka   │◄─►│Conflict │ │
│ │    19c      │   │  KConnect   │   │   KRaft   │   │Resolver │ │
│ │  (Central)  │   │  (Hub CDC)  │   │   (Hub)   │   │ Service │ │
│ └─────────────┘ └──────────────┘ └─────────────┘ └──────────┘ │
└─────────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
    MirrorMaker2        MirrorMaker2        MirrorMaker2
    (Active)            (Active)            (Active)
        │                   │                   │
   ┌────▼─────┐        ┌────▼─────┐        ┌────▼─────┐
   │ BRANCH 1  │        │ BRANCH 2  │        │ BRANCH N  │
   │ Intermit. │        │ Intermit. │        │ Intermit. │
   │           │        │           │        │           │
   │ ┌──────┐  │        │ ┌──────┐  │        │ ┌──────┐  │
   │ │Oracle│  │        │ │Oracle│  │        │ │Oracle│  │
   │ │ 19c  │  │        │ │ 19c  │  │        │ │ 19c  │  │
   │ └──────┘  │        │ └──────┘  │        │ └──────┘  │
   │ [Kafka]   │        │ [Kafka]   │        │ [Kafka]   │
   └───────────┘        └───────────┘        └───────────┘
```

### 1.3 Data Flow

1.  **Hub CDC**: Debezium captures changes from the hub Oracle database and publishes them to designated Kafka topics on the hub cluster.
2.  **Branch CDC**: Similarly, Debezium captures changes from each branch Oracle database and publishes them to local Kafka topics on the respective branch clusters.
3.  **MirrorMaker2 (Branch to Hub)**: MirrorMaker2 instances on each branch replicate the local branch Kafka topics (containing branch-specific changes) to the hub Kafka cluster. These topics are prefixed to indicate their origin (e.g., `hub.BRANCH_1_oracle.transactional.z12026`).
4.  **Conflict Resolver**: A Spring Boot service consumes these replicated branch topics from the hub Kafka cluster. It detects conflicts (e.g., same primary key updated on different branches) and resolves them using the LWW strategy based on Oracle SCN. The resolved changes are then published to new 
Kafka topics (e.g., `oracle-sync.resolved.z12026`).
5.  **Hub Sink**: A Debezium JDBC Sink Connector on the hub consumes the `oracle-sync.resolved.*` topics and applies the resolved changes to the hub Oracle database.
6.  **MirrorMaker2 (Hub to Branch)**: MirrorMaker2 instances on the hub replicate the `oracle-sync.resolved.*` topics back to all connected branch Kafka clusters. This ensures that all branches eventually receive the globally resolved state.

## 2. Technology Stack

The solution is built upon a robust set of open-source technologies:

| Component          | Version           | Purpose                                        |
| :----------------- | :---------------- | :--------------------------------------------- |
| Oracle Database    | 19c               | Primary data store (Hub and Branches)          |
| Apache Kafka       | 4.1.1 with KRaft  | Distributed streaming platform, message broker |
| Debezium           | 3.5.0             | Change Data Capture (CDC) platform             |
| Debezium JDBC Sink | Latest            | Kafka Connect sink for JDBC-compatible databases |
| Kafka MirrorMaker2 | Kafka native      | Cross-cluster data replication                 |
| Spring Boot        | 3.x               | Framework for conflict resolution service      |
| Docker             | Latest            | Containerization and orchestration             |
| OpenVPN            | Configured        | Secure network connectivity for branches       |

## 3. Project Structure

The project is organized into several logical directories, each containing related components:

```
.
├── README.md                          # Project overview and quick start
├── .env.example                       # Template for environment variables
├── .gitignore                         # Specifies intentionally untracked files
├── LICENSE                            # Project license (MIT)
│
├── connectors/                        # Kafka Connect connector configurations
│   ├── hub/                           # Hub-specific connector configurations
│   │   ├── oracle-source-hub.json    # Debezium Oracle source connector for hub
│   │   ├── oracle-sink-hub.json      # JDBC sink connector for hub
│   │   └── register-connectors-hub.sh # Script to register hub connectors
│   ├── branch/                        # Branch-specific connector configurations
│   │   ├── oracle-source-branch.json # Debezium Oracle source connector for branch
│   │   ├── oracle-sink-branch.json   # JDBC sink connector for branch
│   │   └── register-connectors-branch.sh # Script to register branch connectors
│   └── mirrormaker/                   # MirrorMaker2 configurations
│       ├── mm2-hub-to-branch.properties # MM2 configuration for hub to branch replication
│       ├── mm2-branch-to-hub.properties # MM2 configuration for branch to hub replication
│       └── mm2-connect-cluster.properties # MM2 cluster configuration
│
├── conflict-resolver/                 # Spring Boot application for conflict resolution
│   ├── src/main/java/com/sync/resolver/
│   │   ├── ConflictResolverApp.java   # Main application class
│   │   ├── config/KafkaConfig.java    # Kafka configuration for the resolver
│   │   ├── model/                     # Data models for change events and conflicts
│   │   │   ├── ChangeEvent.java
│   │   │   └── ConflictRecord.java
│   │   ├── service/                   # Business logic for conflict resolution
│   │   │   ├── ConflictResolverService.java
│   │   │   └── OffsetTrackingService.java
│   │   └── processor/ChangeEventProcessor.java # Processes change events
│   └── pom.xml                        # Maven project file
│
├── dashboard/                         # React-based monitoring dashboard
│   ├── src/components/                # React components for dashboard widgets
│   │   ├── BranchStatus.jsx           # Displays branch connectivity status
│   │   ├── SyncLag.jsx                # Shows synchronization lag per branch
│   │   └── ConflictLog.jsx            # Logs and displays conflict resolution events
│   └── package.json                   # Node.js package file
│
├── docker/                            # Docker Compose files and related configurations
│   ├── hub/                           # Docker Compose for hub services
│   │   ├── docker-compose-hub.yml     # Defines Kafka, Connect, Prometheus, Grafana for hub
│   │   ├── kafka/server-hub.properties # Kafka broker properties for hub
│   │   ├── grafana/                   # Grafana provisioning files
│   │   └── prometheus/                # Prometheus configuration files
│   └── branch/                        # Docker Compose for branch services
│       ├── docker-compose-branch.yml  # Defines Kafka, Connect, MirrorMaker2 for branch
│       ├── kafka/server-branch.properties # Kafka broker properties for branch
│       └── mirrormaker/Dockerfile     # Dockerfile for MirrorMaker2 on branch
│
├── monitoring-api/                    # Spring Boot REST API for monitoring data
│   ├── src/main/java/com/sync/monitor/
│   │   ├── controller/                # REST controllers for branch and sync status
│   │   │   ├── BranchController.java
│   │   │   └── SyncStatusController.java
│   │   └── service/BranchRegistryService.java # Manages registered branches
│   └── pom.xml                        # Maven project file
│
├── scripts/                           # Shell scripts for setup, maintenance, and operations
│   ├── oracle/                        # Oracle-specific SQL and shell scripts
│   │   ├── hub/                       # Oracle setup scripts for hub
│   │   │   ├── 01-enable-logminer.sql # Enables Oracle LogMiner
│   │   │   ├── 02-create-sync-user.sql # Creates Debezium sync user
│   │   │   └── 03-supplemental-logging.sql # Configures supplemental logging
│   │   └── branch/                    # Oracle setup scripts for branch
│   │       ├── 01-enable-logminer.sql
│   │       ├── 02-create-sync-user.sql
│   │       ├── 03-supplemental-logging.sql
│   │       └── 04-conflict-columns.sql # Adds metadata columns for conflict resolution
│   ├── setup-hub.sh                   # Automates hub server setup
│   ├── setup-branch.sh                # Automates branch server setup
│   ├── add-new-branch.sh              # Script to register a new branch with the hub
│   ├── health-check.sh                # Performs system health checks
│   ├── restart-connectors.sh          # Restarts Kafka Connect connectors
│   └── backup-kafka-offsets.sh        # Backs up Kafka consumer offsets
│
└── systemd/                           # SystemD service unit files for production deployment
    ├── hub/                           # SystemD services for hub components
    │   ├── kafka-hub.service
    │   ├── kafka-connect-hub.service
    │   ├── conflict-resolver.service
    │   └── mirrormaker2-hub.service
    └── branch/                        # SystemD services for branch components
        ├── kafka-branch.service
        ├── kafka-connect-branch.service
        ├── mirrormaker2-branch.service
        └── install-services-branch.sh # Script to install branch SystemD services
```

## 4. Quick Start Guide

This section outlines the steps to quickly set up and run the Oracle Sync General solution.

### 4.1 Prerequisites

Before proceeding, ensure the following prerequisites are met on both hub and branch servers:

*   **Operating System**: Oracle Linux 8 (or compatible Linux distribution)
*   **Oracle Database**: Oracle 19c installed and configured.
*   **Docker & Docker Compose**: Latest versions installed for container orchestration.
*   **OpenVPN**: Configured and running for secure communication between hub and branch servers.
*   **Java Development Kit (JDK)**: Version 11 or higher (for Spring Boot applications).
*   **Apache Maven**: Version 3.6 or higher (for building Java projects).
*   **`sqlplus`**: Oracle SQL*Plus client installed and accessible in the PATH.
*   **`jq`**: Command-line JSON processor (for health checks).

### 4.2 Initial Setup

1.  **Clone the Repository**:

    ```bash
    git clone https://github.com/aimanabsi/oracle-sync-general.git
    cd oracle-sync-general
    ```

2.  **Configure Environment Variables**:

    Copy the example environment file and populate it with your specific Oracle credentials and network settings. This file will be sourced by the setup scripts.

    ```bash
    cp .env.example .env
    # Open .env in a text editor and fill in the required values
    ```

    **Important**: Ensure `ORACLE_HUB_SYS_PASSWORD`, `ORACLE_HUB_SYNC_USER`, `ORACLE_HUB_SYNC_PASSWORD`, and similar branch-specific variables are correctly set.

### 4.3 Hub Server Setup

Execute the `setup-hub.sh` script on your designated hub server. This script automates the Oracle configuration, starts Docker services (Kafka, Kafka Connect, Conflict Resolver, Prometheus, Grafana), and registers the necessary Debezium connectors.

```bash
./scripts/setup-hub.sh
```

Upon successful execution, the script will output the URLs for accessing the hub services.

### 4.4 Branch Server Setup

Execute the `setup-branch.sh` script on each of your designated branch servers. This script configures the branch Oracle database, starts local Kafka and Kafka Connect services, and registers branch-specific Debezium connectors and MirrorMaker2.

```bash
./scripts/setup-branch.sh
```

### 4.5 Adding a New Branch

To integrate additional branch servers into the synchronization system, use the `add-new-branch.sh` script on the **hub server**. This script will register the new branch with the hub, ensuring that MirrorMaker2 configurations are updated to include the new branch for replication.

```bash
./scripts/add-new-branch.sh --branch-name BRANCH_2 --branch-ip 192.168.1.100
```

**Note**: The `add-new-branch.sh` script is a placeholder and needs to be implemented to dynamically update MirrorMaker2 configurations and potentially other components to include the new branch.

## 5. Oracle Configuration Details

This section provides detailed SQL scripts and explanations for configuring Oracle 19c on both hub and branch servers to support Debezium CDC.

### 5.1 Hub Server Oracle Setup

Connect to your Oracle 19c database as `SYSDBA` to execute the following scripts.

1.  **Enable LogMiner and Archive Mode** (`scripts/oracle/hub/01-enable-logminer.sql`):

    This script configures the database for Change Data Capture by enabling archive logging, setting up a recovery area, and creating a dedicated tablespace for LogMiner.

    ```sql
    -- Connect as SYSDBA
    sqlplus sys as sysdba

    -- Set recovery area size and location
    ALTER SYSTEM SET db_recovery_file_dest_size = 20G;
    ALTER SYSTEM SET db_recovery_file_dest = '/u01/app/oracle/oradata/recovery_area' SCOPE=SPFILE;

    -- Create LogMiner tablespace on CDB
    CREATE TABLESPACE LOGMINER_TBS DATAFILE
      '/u01/app/oracle/oradata/ORCLCDB/logminer_tbs.dbf' SIZE 25M 
      REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;

    -- Create LogMiner tablespace on PDB (assuming ORCLPDB is your PDB name)
    CREATE TABLESPACE LOGMINER_TBS DATAFILE
      '/u01/app/oracle/oradata/ORCLCDB/orclpdb/logminer_tbs.dbf' SIZE 25M 
      REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;

    -- Enable archive mode
    ALTER SYSTEM SET log_archive_dest_1='LOCATION=/u01/app/oracle/oradata/recovery_area VALID_FOR=(ALL_LOGFILES,ALL_ROLES) DB_UNIQUE_NAME=ORCLCDB' SCOPE=SPFILE;

    -- Enable supplemental logging for all columns (recommended for Debezium)
    ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
    ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;
    ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (UNIQUE KEY) COLUMNS;
    ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (FOREIGN KEY) COLUMNS;
    ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

    -- Restart database to apply changes (requires downtime)
    SHUTDOWN IMMEDIATE;
    STARTUP;

    -- Verify LogMiner and archive mode status
    SELECT SUPPLEMENTAL_LOG_DATA_MIN, SUPPLEMENTAL_LOG_DATA_PK, 
           SUPPLEMENTAL_LOG_DATA_UI, SUPPLEMENTAL_LOG_DATA_FK, 
           SUPPLEMENTAL_LOG_DATA_ALL
    FROM V$DATABASE;
    ALTER DATABASE ARCHIVELOG;
    ARCHIVE LOG LIST;
    ```

2.  **Create Debezium Sync User** (`scripts/oracle/hub/02-create-sync-user.sql`):

    A dedicated Oracle user (`debezium`) is created with the necessary privileges to access the transaction logs and the `Z12026` and `ZLAB` schemas.

    ```sql
    -- Connect to PDB as SYSDBA
    ALTER SESSION SET CONTAINER=ORCLPDB;

    -- Create sync user and grant basic privileges
    CREATE USER debezium IDENTIFIED BY OraclePa$$w0rd;
    GRANT CREATE SESSION, CONNECT, RESOURCE TO debezium;

    -- Grant SELECT on all tables in Z12026 and ZLAB schemas
    GRANT SELECT ON Z12026.* TO debezium;
    GRANT SELECT ON ZLAB.* TO debezium;

    -- Grant LogMiner and DBA view privileges (essential for Debezium)
    GRANT EXECUTE ON DBMS_LOGMNR TO debezium;
    GRANT EXECUTE ON DBMS_LOGMNR_D TO debezium;
    GRANT SELECT ON V_$LOG, V_$LOGFILE, V_$ARCHIVED_LOG, V_$ARCHIVE_DEST, 
          V_$DATABASE, V_$THREAD, V_$PARAMETER, V_$NLS_PARAMETERS, 
          V_$TIMEZONE_NAMES, V_$TRANSACTION, V_$ROLLNAME, V_$INSTANCE, 
          V_$LOG_HISTORY TO debezium;
    GRANT SELECT ON DBA_OBJECTS, DBA_TABLES, DBA_TAB_COLUMNS, DBA_CONSTRAINTS, 
          DBA_CONS_COLUMNS, DBA_INDEXES, DBA_IND_COLUMNS, DBA_SEQUENCES, 
          DBA_SYNONYMS, DBA_VIEWS, DBA_TAB_PRIVS, DBA_ROLE_PRIVS, DBA_USERS, 
          DBA_SEGMENTS, DBA_EXTENTS TO debezium;

    -- Grant ALTER SYSTEM/DATABASE for supplemental logging (if not already done by SYSDBA)
    GRANT ALTER SYSTEM, ALTER DATABASE TO debezium;

    -- Verify user creation
    SELECT username, account_status FROM dba_users WHERE username='DEBEZIUM';
    ```

3.  **Configure Supplemental Logging** (`scripts/oracle/hub/03-supplemental-logging.sql`):

    This script ensures that Oracle logs all necessary column changes for the `Z12026` and `ZLAB` schemas, which is crucial for Debezium to capture complete change events.

    ```sql
    -- Connect to PDB as SYSDBA
    ALTER SESSION SET CONTAINER=ORCLPDB;

    -- Enable database-level supplemental logging (if not already done)
    ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
    ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;
    ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (UNIQUE KEY) COLUMNS;
    ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (FOREIGN KEY) COLUMNS;
    ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

    -- Enable supplemental logging for all tables in Z12026 schema
    BEGIN
      FOR tab IN (SELECT table_name FROM dba_tables WHERE owner='Z12026')
      LOOP
        EXECUTE IMMEDIATE 'ALTER TABLE Z12026.' || tab.table_name || ' ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS';
      END LOOP;
    END;
    /

    -- Enable supplemental logging for all tables in ZLAB schema
    BEGIN
      FOR tab IN (SELECT table_name FROM dba_tables WHERE owner='ZLAB')
      LOOP
        EXECUTE IMMEDIATE 'ALTER TABLE ZLAB.' || tab.table_name || ' ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS';
      END LOOP;
    END;
    /

    -- Verify supplemental logging status
    SELECT owner, table_name, log_group_type, always
    FROM dba_log_groups
    WHERE owner IN ('Z12026', 'ZLAB')
    ORDER BY owner, table_name;
    ```

### 5.2 Branch Server Oracle Setup

The Oracle configuration on branch servers is similar to the hub, with an additional step to add metadata columns for conflict resolution. The scripts `01-enable-logminer.sql`, `02-create-sync-user.sql`, and `03-supplemental-logging.sql` should be adapted for the branch environment (e.g., using `ORACLE_BRANCH_PDB` and `ORACLE_BRANCH_SYS_PASSWORD`).

1.  **Add Conflict Metadata Columns** (`scripts/oracle/branch/04-conflict-columns.sql`):

    This script adds four new columns (`_sync_scn`, `_sync_timestamp`, `_sync_source`, `_sync_branch`) to all tables within the `Z12026` and `ZLAB` schemas. These columns are used by the conflict resolution service to track the origin and version of changes.

    ```sql
    -- Connect to PDB as SYSDBA
    ALTER SESSION SET CONTAINER=ORCLPDB;

    -- Add metadata columns to all tables in Z12026 schema
    BEGIN
      FOR tab IN (SELECT table_name FROM dba_tables WHERE owner='Z12026')
      LOOP
        BEGIN
          EXECUTE IMMEDIATE 'ALTER TABLE Z12026.' || tab.table_name || 
            ' ADD (_sync_scn NUMBER, _sync_timestamp TIMESTAMP, _sync_source VARCHAR2(50), _sync_branch VARCHAR2(50))';
          DBMS_OUTPUT.PUT_LINE('Added columns to Z12026.' || tab.table_name);
        EXCEPTION
          WHEN OTHERS THEN
            IF SQLCODE != -1430 THEN  -- ORA-01430: column already exists
              DBMS_OUTPUT.PUT_LINE('Error on Z12026.' || tab.table_name || ': ' || SQLERRM);
            END IF;
        END;
      END LOOP;
    END;
    /

    -- Add metadata columns to all tables in ZLAB schema
    BEGIN
      FOR tab IN (SELECT table_name FROM dba_tables WHERE owner='ZLAB')
      LOOP
        BEGIN
          EXECUTE IMMEDIATE 'ALTER TABLE ZLAB.' || tab.table_name || 
            ' ADD (_sync_scn NUMBER, _sync_timestamp TIMESTAMP, _sync_source VARCHAR2(50), _sync_branch VARCHAR2(50))';
          DBMS_OUTPUT.PUT_LINE('Added columns to ZLAB.' || tab.table_name);
        EXCEPTION
          WHEN OTHERS THEN
            IF SQLCODE != -1430 THEN  -- ORA-01430: column already exists
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
    ```

## 6. Kafka Topics

Kafka topics are central to the data flow, acting as conduits for change events. The following table outlines the key topics and their purposes:

### 6.1 Hub Kafka Topics

| Topic                                        | Source            | Purpose                                                               |
| :------------------------------------------- | :---------------- | :-------------------------------------------------------------------- |
| `oracle-sync.transactional.z12026`           | Hub CDC           | Raw change events from the `Z12026` schema on the hub.                |
| `oracle-sync.transactional.zlab`             | Hub CDC           | Raw change events from the `ZLAB` schema on the hub.                  |
| `hub.BRANCH_1_oracle.transactional.z12026`   | MM2 (Branch 1)    | Replicated change events from Branch 1 for `Z12026` schema.           |
| `hub.BRANCH_1_oracle.transactional.zlab`     | MM2 (Branch 1)    | Replicated change events from Branch 1 for `ZLAB` schema.             |
| `hub.BRANCH_2_oracle.transactional.z12026`   | MM2 (Branch 2)    | Replicated change events from Branch 2 for `Z12026` schema.           |
| `hub.BRANCH_2_oracle.transactional.zlab`     | MM2 (Branch 2)    | Replicated change events from Branch 2 for `ZLAB` schema.             |
| `oracle-sync.resolved.z12026`                | Conflict Resolver | Resolved change events for `Z12026` schema after conflict resolution. |
| `oracle-sync.resolved.zlab`                  | Conflict Resolver | Resolved change events for `ZLAB` schema after conflict resolution.   |

### 6.2 Branch Kafka Topics

Each branch server will have its own local Kafka topics for capturing changes before they are replicated to the hub. These topics typically follow a similar naming convention but are local to the branch Kafka cluster.

## 7. Conflict Resolution

The conflict resolution service is a critical component that ensures data consistency in a multi-master environment. It employs a **Last-Write-Wins (LWW)** strategy, leveraging Oracle System Change Numbers (SCN) to determine the most recent change.

### 7.1 Resolution Logic

1.  **Detection**: The conflict resolver monitors the replicated branch topics on the hub Kafka cluster. When it detects multiple updates to the same primary key from different branches, a conflict is identified.
2.  **Comparison**: For conflicting records, the service compares the `_sync_scn` (System Change Number) metadata column. The record with the highest SCN is considered the 
winner.
3.  **Resolution**: The winning record is then published to a dedicated `oracle-sync.resolved.*` Kafka topic.
4.  **Broadcasting**: MirrorMaker2 replicates this resolved record back to all connected branch Kafka clusters, ensuring that all branches eventually converge to the same consistent state.

### 7.2 Example Scenario

Consider a scenario where two branches update the same patient record concurrently while offline:

```
Timeline:
T1: Branch-A updates Patient ID=100, Name="Ali Ahmed" (offline)
T2: Branch-B updates Patient ID=100, Name="Ali Mohamed" (offline)
T3: Both branches reconnect to Hub
T4: Both updates arrive at Hub Kafka topics (e.g., hub.BRANCH_A_oracle.transactional.patients, hub.BRANCH_B_oracle.transactional.patients)
T5: Conflict Resolver detects SAME primary key (Patient ID=100) from different sources = CONFLICT!
T6: Resolver compares SCNs. Assuming Branch-B's update has a higher SCN, Branch-B wins.
T7: Resolver publishes the winning record (Patient ID=100, Name="Ali Mohamed") to oracle-sync.resolved.patients
T8: oracle-sink-hub.json writes "Ali Mohamed" to Hub Oracle
T9: Hub Oracle now has the resolved, correct record
T10: MM2 replicates the resolved record back to ALL branches, ensuring consistency.
```

## 8. Monitoring

Effective monitoring is crucial for maintaining the health and performance of the synchronization system. The solution includes components for real-time monitoring and alerting.

### 8.1 Dashboard

A React-based monitoring dashboard provides a centralized view of the system's status. It can be accessed via a web browser, typically at `http://localhost:3000` (if running locally or exposed via Docker).

**Key Dashboard Features**:

*   **Branch Connectivity Status**: Real-time status of each connected branch (online/offline).
*   **Sync Lag**: Displays the synchronization lag for each branch, indicating how far behind a branch is from the hub or vice versa.
*   **Conflict Log**: A detailed log of all detected and resolved conflicts, including timestamps, affected records, and resolution outcomes.
*   **Kafka Topic Metrics**: Visualizations of Kafka topic throughput, consumer group lags, and message rates.
*   **Oracle Transaction Rates**: Metrics on transaction activity in both hub and branch Oracle databases.

### 8.2 Health Checks

The `health-check.sh` script provides a command-line utility to quickly assess the health of various components in the system.

```bash
./scripts/health-check.sh
```

This script verifies:

*   **Kafka Broker Health**: Checks if Kafka brokers are running and responsive.
*   **Debezium Connector Status**: Reports the status of all registered Debezium source and sink connectors.
*   **Oracle Database Connectivity**: Basic connectivity check to Oracle databases (requires manual credential input for full verification).
*   **MirrorMaker2 Replication Lag**: Monitors the lag in data replication between clusters.
*   **Conflict Resolver Service Health**: Checks the health endpoint of the Spring Boot conflict resolver.
*   **Docker Container Status**: Lists the running status of all relevant Docker containers.

## 9. Troubleshooting

This section provides guidance on common issues and their resolution.

### 9.1 Connector Issues

If Debezium connectors are not functioning correctly, follow these steps:

*   **Check Connector Status**:
    ```bash
    curl http://localhost:8083/connectors
    curl http://localhost:8083/connectors/<connector-name>/status
    ```
*   **View Connector Logs**:
    ```bash
    docker logs kafka-connect-hub
    # or for branch
    docker logs kafka-connect-branch
    ```
*   **Restart Connector**:
    ```bash
    ./scripts/restart-connectors.sh <connector-name>
    # Example: ./scripts/restart-connectors.sh oracle-source-hub
    ```

### 9.2 Sync Lag

High synchronization lag indicates that data changes are not being processed quickly enough. Possible causes and solutions include:

*   **Check Lag per Branch**:
    ```bash
    ./scripts/health-check.sh --lag
    # Monitor in real-time
    watch -n 5 './scripts/health-check.sh --lag'
    ```
*   **Resource Constraints**: Ensure Kafka brokers, Kafka Connect, and Oracle databases have sufficient CPU, memory, and disk I/O.
*   **Network Latency**: High latency between hub and branches can impact MirrorMaker2 performance. Verify OpenVPN connection quality.
*   **Debezium Configuration**: Adjust `max.queue.size`, `max.batch.size`, `poll.interval.ms` in Debezium connector configurations.
*   **JDBC Sink Batch Size**: Optimize `batch.size` in JDBC Sink Connector for better throughput.

### 9.3 Conflict Resolution Failures

If conflicts are not being resolved or are leading to data inconsistencies:

*   **Check Conflict Resolver Logs**:
    ```bash
    docker logs conflict-resolver
    ```
*   **Verify SCN Tracking**: Ensure that the `_sync_scn` column is correctly populated in the branch databases and that the conflict resolver is correctly interpreting SCN values.
*   **Review Conflict Strategy**: Confirm that the LWW strategy is appropriate for your data and business rules. Consider custom resolution logic if needed.

## 10. Contributing

Contributions to the Oracle Sync General project are welcome! Please follow these guidelines:

1.  **Fork the repository**.
2.  **Create a feature branch**: `git checkout -b feature/your-feature`.
3.  **Commit your changes**: `git commit -am 'Add your feature'`.
4.  **Push to the branch**: `git push origin feature/your-feature`.
5.  **Submit a pull request**.

## 11. License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## 12. Support

For any issues, questions, or feature requests, please open an issue on the GitHub repository or contact the project maintainers.

## 13. Changelog

### Version 1.0.0 (Initial Release)

*   Implemented hub-and-spoke architecture for bidirectional Oracle synchronization.
*   Integrated Kafka, Debezium CDC, and MirrorMaker2.
*   Developed Spring Boot-based conflict resolution with Last-Write-Wins (LWW) strategy.
*   Provided comprehensive Oracle setup scripts for hub and branch environments.
*   Included Docker Compose configurations for easy deployment.
*   Established initial project structure with monitoring and troubleshooting utilities.
