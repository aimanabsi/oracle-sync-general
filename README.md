# Oracle Sync General - Bidirectional Multi-Branch Synchronization

A comprehensive solution for synchronizing Oracle 19c databases across multiple branches with intermittent connectivity using Kafka, Debezium CDC, and MirrorMaker2.

## Project Overview

This project implements a **hub-and-spoke architecture** for bidirectional data synchronization between a central Oracle database (always online) and multiple branch servers that may experience intermittent network connectivity (1-45 hours of disconnection).

### Key Features

- **Bidirectional Synchronization**: Changes flow from branches to hub and vice versa
- **Multi-Branch Support**: Easily add multiple branch servers (Branch1, Branch2, BranchN)
- **Conflict Resolution**: Automatic conflict detection and resolution using Last-Write-Wins (LWW) with SCN
- **Offline Buffering**: Local Kafka buffers store changes during network disconnection
- **Schema Support**: Synchronizes Z12026 and ZLAB schemas with all their tables
- **Order Preservation**: Maintains referential integrity across master-detail relationships
- **Performance Optimized**: Tuned for resource efficiency and minimal latency
- **Monitoring & Dashboard**: Real-time sync status, conflict logs, and branch connectivity

## Architecture

### System Components

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

### Data Flow

1. **Hub CDC**: Debezium captures changes from hub Oracle → Kafka topics
2. **Branch CDC**: Debezium captures changes from branch Oracle → Local Kafka
3. **MirrorMaker2**: Replicates branch topics to hub Kafka cluster
4. **Conflict Resolver**: Detects and resolves conflicts using SCN-based LWW
5. **Hub Sink**: Writes resolved changes back to hub Oracle
6. **Broadcast**: MirrorMaker2 replicates resolved changes back to all branches

## Technology Stack

| Component | Version | Purpose |
|-----------|---------|---------|
| Oracle Database | 19c | Source and target database |
| Kafka | 4.1.1 with KRaft | Message broker and event streaming |
| Debezium | 3.5.0 | Change Data Capture (CDC) |
| Debezium JDBC Sink | Latest | Write changes to Oracle |
| MirrorMaker2 | Kafka native | Cross-cluster replication |
| Spring Boot | 3.x | Conflict resolution service |
| Docker | Latest | Container orchestration |

## Project Structure

```
.
├── README.md                          # This file
├── .env.example                       # Environment variables template
├── .gitignore                         # Git ignore patterns
├── LICENSE                            # Project license
│
├── connectors/                        # Kafka Connect configurations
│   ├── hub/
│   │   ├── oracle-source-hub.json    # Hub CDC source connector
│   │   ├── oracle-sink-hub.json      # Hub sink connector
│   │   └── register-connectors-hub.sh
│   ├── branch/
│   │   ├── oracle-source-branch.json # Branch CDC source
│   │   ├── oracle-sink-branch.json   # Branch sink
│   │   └── register-connectors-branch.sh
│   └── mirrormaker/
│       ├── mm2-hub-to-branch.properties
│       ├── mm2-branch-to-hub.properties
│       └── mm2-connect-cluster.properties
│
├── conflict-resolver/                 # Spring Boot conflict resolution
│   ├── src/main/java/com/sync/resolver/
│   │   ├── ConflictResolverApp.java
│   │   ├── config/KafkaConfig.java
│   │   ├── model/
│   │   │   ├── ChangeEvent.java
│   │   │   └── ConflictRecord.java
│   │   ├── service/
│   │   │   ├── ConflictResolverService.java
│   │   │   └── OffsetTrackingService.java
│   │   └── processor/ChangeEventProcessor.java
│   └── pom.xml
│
├── dashboard/                         # React monitoring dashboard
│   ├── src/components/
│   │   ├── BranchStatus.jsx
│   │   ├── SyncLag.jsx
│   │   └── ConflictLog.jsx
│   └── package.json
│
├── docker/                            # Docker configurations
│   ├── hub/
│   │   ├── docker-compose-hub.yml
│   │   ├── kafka/server-hub.properties
│   │   ├── grafana/
│   │   └── prometheus/
│   └── branch/
│       ├── docker-compose-branch.yml
│       ├── kafka/server-branch.properties
│       └── mirrormaker/Dockerfile
│
├── monitoring-api/                    # Monitoring REST API
│   ├── src/main/java/com/sync/monitor/
│   │   ├── controller/
│   │   │   ├── BranchController.java
│   │   │   └── SyncStatusController.java
│   │   └── service/BranchRegistryService.java
│   └── pom.xml
│
├── scripts/                           # Setup and maintenance scripts
│   ├── oracle/
│   │   ├── hub/
│   │   │   ├── 01-enable-logminer.sql
│   │   │   ├── 02-create-sync-user.sql
│   │   │   └── 03-supplemental-logging.sql
│   │   └── branch/
│   │       ├── 01-enable-logminer.sql
│   │       ├── 02-create-sync-user.sql
│   │       ├── 03-supplemental-logging.sql
│   │       └── 04-conflict-columns.sql
│   ├── setup-hub.sh
│   ├── setup-branch.sh
│   ├── add-new-branch.sh
│   ├── health-check.sh
│   ├── restart-connectors.sh
│   └── backup-kafka-offsets.sh
│
└── systemd/                           # SystemD service files
    ├── hub/
    │   ├── kafka-hub.service
    │   ├── kafka-connect-hub.service
    │   ├── conflict-resolver.service
    │   └── mirrormaker2-hub.service
    └── branch/
        ├── kafka-branch.service
        ├── kafka-connect-branch.service
        ├── mirrormaker2-branch.service
        └── install-services-branch.sh
```

## Quick Start

### Prerequisites

- Oracle Linux 8 with Oracle 19c installed
- Docker and Docker Compose
- OpenVPN configured for branch connectivity
- Java 11+ installed
- Maven 3.6+ (for building services)

### 1. Clone the Repository

```bash
git clone https://github.com/aimanabsi/oracle-sync-general.git
cd oracle-sync-general
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your Oracle credentials and network settings
```

### 3. Setup Hub Server

```bash
./scripts/setup-hub.sh
```

This script will:
- Enable LogMiner on hub Oracle
- Create sync user and tablespaces
- Enable supplemental logging
- Start Kafka and Debezium connectors

### 4. Setup Branch Server

```bash
./scripts/setup-branch.sh
```

### 5. Add New Branch

```bash
./scripts/add-new-branch.sh --branch-name BRANCH_2 --branch-ip 192.168.1.100
```

## Oracle Configuration

### Hub Server Setup

```sql
-- Connect as SYSDBA
sqlplus sys as sysdba

-- Enable archive mode and recovery
alter system set db_recovery_file_dest_size = 20G;
alter system set db_recovery_file_dest = '/u01/app/oracle/oradata/recovery_area' scope=spfile;

-- Create LogMiner tablespace
CREATE TABLESPACE LOGMINER_TBS DATAFILE
  '/u01/app/oracle/oradata/ORCLCDB/logminer_tbs.dbf' SIZE 25M 
  REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;

-- Enable LogMiner for CDC
EXEC DBMS_LOGMNR_D.SET_TABLESPACE('LOGMINER_TBS');
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
```

### Branch Server Setup

Similar to hub, but with branch-specific configurations for handling offline scenarios.

## Kafka Topics

### Hub Topics

| Topic | Source | Purpose |
|-------|--------|---------|
| `oracle-sync.transactional.z12026` | Hub CDC | Z12026 schema changes |
| `oracle-sync.transactional.zlab` | Hub CDC | ZLAB schema changes |
| `hub.BRANCH_1_oracle.transactional.z12026` | MM2 | Branch1 changes replicated to hub |
| `hub.BRANCH_2_oracle.transactional.z12026` | MM2 | Branch2 changes replicated to hub |
| `oracle-sync.resolved.z12026` | Conflict Resolver | Resolved changes for z12026 |
| `oracle-sync.resolved.zlab` | Conflict Resolver | Resolved changes for zlab |

## Conflict Resolution

The system uses **Last-Write-Wins (LWW)** strategy with Oracle SCN (System Change Number):

1. **Detection**: When same primary key appears in multiple topics
2. **Comparison**: Compare SCN values to determine which update is newer
3. **Resolution**: Apply the update with highest SCN
4. **Broadcasting**: Replicate resolved record to all branches

### Example Scenario

```
Timeline:
T1: Branch-A updates Patient ID=100, Name="Ali Ahmed" (offline)
T2: Branch-B updates Patient ID=100, Name="Ali Mohamed" (offline)
T3: Both branches reconnect to Hub
T4: Both updates arrive at Hub Kafka topics
T5: Conflict Resolver detects SAME primary key = CONFLICT!
T6: Resolver picks winner (highest SCN = Branch-B wins)
T7: Resolver publishes to oracle-sync.resolved.patients
T8: oracle-sink-hub.json writes "Ali Mohamed" to Hub Oracle
T9: Hub Oracle now has the resolved, correct record
T10: MM2 replicates resolved record back to ALL branches
```

## Monitoring

### Dashboard

Access the monitoring dashboard at `http://localhost:3000`

Features:
- Real-time branch connectivity status
- Per-branch sync lag metrics
- Conflict resolution logs
- Kafka topic lag monitoring
- Oracle transaction rates

### Health Checks

```bash
./scripts/health-check.sh
```

Verifies:
- Kafka broker health
- Debezium connector status
- Oracle database connectivity
- MirrorMaker2 replication lag

## Performance Considerations

### Ordering Guarantees

- **Per-partition ordering**: Maintained through Kafka partitioning by primary key
- **Master-detail relationships**: Partitioned by master table key to preserve order
- **Composite keys**: Handled through consistent hashing

### Resource Optimization

- **Kafka**: KRaft mode reduces resource overhead
- **Debezium**: Batch size tuned for Oracle 19c
- **JDBC Sink**: Connection pooling and batch inserts
- **Conflict Resolver**: Single-threaded processing per partition

### Network Resilience

- **Offline buffering**: Local Kafka retains changes for up to 45 hours
- **Automatic reconnection**: MirrorMaker2 resumes when network restored
- **Offset tracking**: Ensures no duplicate or missed changes

## Troubleshooting

### Connector Issues

```bash
# Check connector status
curl http://localhost:8083/connectors

# View connector logs
docker logs kafka-connect-hub

# Restart connector
./scripts/restart-connectors.sh
```

### Sync Lag

```bash
# Check lag per branch
./scripts/health-check.sh --lag

# Monitor in real-time
watch -n 5 './scripts/health-check.sh --lag'
```

### Conflict Resolution Failures

Check the conflict resolver logs:

```bash
docker logs conflict-resolver
```

## Contributing

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Commit changes: `git commit -am 'Add your feature'`
3. Push to branch: `git push origin feature/your-feature`
4. Submit a pull request

## License

This project is licensed under the MIT License - see LICENSE file for details.

## Support

For issues, questions, or contributions, please open an issue on GitHub or contact the project maintainers.

## Changelog

### Version 1.0.0 (Initial Release)

- Hub-and-spoke architecture
- Bidirectional sync with conflict resolution
- Multi-branch support
- Monitoring dashboard
- Complete Oracle setup scripts
