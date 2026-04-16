# Oracle Sync General - Bidirectional Multi-Branch Oracle Synchronization System

This document provides a comprehensive implementation guide for setting up a bidirectional multi-branch Oracle database synchronization system. The system is designed with a hub-and-spoke topology, featuring a central Oracle 19c server (hub) and multiple branch servers with intermittent connectivity. It leverages Apache Kafka, Debezium CDC connectors, MirrorMaker2, and a custom Spring Boot-based conflict resolution service.

## Table of Contents
1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Technology Stack](#3-technology-stack)
4. [Project Structure](#4-project-structure)
5. [Prerequisites](#5-prerequisites)
6. [Quick Start Guide](#6-quick-start-guide)
   - [Hub Setup](#hub-setup)
   - [Branch Setup](#branch-setup)
7. [Detailed Component Guides](#7-detailed-component-guides)
   - [Oracle Database Configuration](#oracle-database-configuration)
   - [Kafka and Debezium Connectors](#kafka-and-debezium-connectors)
   - [MirrorMaker2 Configuration](#mirrormaker2-configuration)
   - [Conflict Resolver Service](#conflict-resolver-service)
   - [Monitoring API](#monitoring-api)
   - [Monitoring Dashboard](#monitoring-dashboard)
   - [SystemD Services](#systemd-services)
   - [Kafka UI](#kafka-ui)
8. [Troubleshooting](#8-troubleshooting)
9. [References](#9-references)

## 1. Project Overview

This project aims to establish a robust and scalable solution for synchronizing data between a central Oracle 19c database (Hub) and multiple remote Oracle 19c databases (Branches). Key features include:

-   **Bidirectional Synchronization**: Data changes from the Hub are replicated to Branches, and changes from Branches are replicated back to the Hub.
-   **Conflict Resolution**: A custom Spring Boot service resolves data conflicts using a Last-Write-Wins (LWW) strategy based on Oracle System Change Number (SCN) and commit timestamps, without requiring modifications to the source schema tables.
-   **Offline Buffering**: Kafka acts as a resilient buffer, allowing branches to synchronize data even with intermittent network connectivity.
-   **Multi-Branch Support**: The architecture is designed to easily scale and accommodate multiple branch databases.
-   **Monitoring**: Integrated monitoring with Prometheus and Grafana provides real-time insights into system health, data lag, and conflict resolution.

## 2. Architecture

The system employs a **hub-and-spoke topology**:

-   **Hub**: A central, always-online Oracle 19c database. It hosts the primary data and acts as the central point for all synchronization. Associated with the Hub are Kafka, Kafka Connect (with Debezium), Schema Registry, Conflict Resolver, Monitoring API, Prometheus, Grafana, and Kafka UI.
-   **Branches**: Multiple remote Oracle 19c databases with potentially intermittent connectivity. Each branch has its own Kafka, Kafka Connect (with Debezium), Schema Registry, and MirrorMaker2 instances.

**Data Flow:**

1.  **Oracle CDC (Debezium)**: Debezium captures Change Data Capture (CDC) events from both Hub and Branch Oracle databases and publishes them to respective Kafka topics.
2.  **Kafka**: Acts as a central message bus, buffering change events and enabling asynchronous communication.
3.  **MirrorMaker2**: Replicates Kafka topics between the Hub and Branch clusters, ensuring bidirectional data flow.
4.  **Conflict Resolver**: A Spring Boot application consumes change events from Kafka, detects conflicts (e.g., simultaneous updates to the same record from different branches), and resolves them using the LWW strategy. The resolved events are then published to a resolved topic.
5.  **Oracle Sink Connectors**: Consume resolved change events from Kafka and apply them to the respective Oracle databases (Hub or Branch).
6.  **Monitoring**: Prometheus collects metrics from Kafka, Kafka Connect, and the Spring Boot services. Grafana visualizes these metrics, providing dashboards for system health, data lag, and conflict resolution.

## 3. Technology Stack

The following technologies are utilized in this project:

-   **Oracle Database 19c**: Relational database for data storage (Hub and Branches).
-   **Apache Kafka 4.1.1 with KRaft**: Distributed streaming platform for handling change events.
-   **Debezium 3.5.0 CDC Connector**: Captures row-level changes from Oracle databases.
-   **MirrorMaker2**: Replicates Kafka topics between clusters.
-   **Spring Boot**: Framework for developing the Conflict Resolver and Monitoring API services.
-   **React**: Frontend framework for the Monitoring Dashboard.
-   **Prometheus**: Monitoring system for collecting metrics.
-   **Grafana**: Data visualization and dashboarding tool.
-   **Docker & Docker Compose**: For containerization and orchestration of services.
-   **SystemD**: For managing services on Linux systems.

## 4. Project Structure

The project is organized into the following directories:

```
oracle-sync-project/
├── .env.example
├── .gitignore
├── README.md
├── IMPLEMENTATION_GUIDE.md
├── conflict-resolver/             # Spring Boot application for conflict resolution
│   ├── src/
│   ├── pom.xml
│   └── Dockerfile
├── connectors/                    # Debezium and MirrorMaker2 connector configurations
│   ├── hub/
│   │   ├── oracle-source-hub.json
│   │   └── oracle-sink-hub.json
│   ├── branch/
│   │   ├── oracle-source-branch.json
│   │   └── oracle-sink-branch.json
│   └── mirrormaker/
│       ├── mm2-hub-to-branch.properties
│       └── mm2-branch-to-hub.properties
├── dashboard/                     # React-based monitoring dashboard
│   ├── public/
│   ├── src/
│   └── package.json
├── monitoring-api/                # Spring Boot application for monitoring data
│   ├── src/
│   ├── pom.xml
│   └── Dockerfile
├── scripts/                       # Shell and SQL scripts for setup and management
│   ├── oracle/
│   │   ├── hub/
│   │   │   ├── 01-enable-logminer.sql
│   │   │   ├── 02-create-sync-user.sql
│   │   │   └── 03-supplemental-logging.sql
│   │   └── branch/
│   │       ├── 01-enable-logminer.sql
│   │       ├── 02-create-sync-user.sql
│   │       └── 03-supplemental-logging.sql
│   ├── setup-hub.sh
│   ├── setup-branch.sh
│   ├── health-check.sh
│   └── add-new-branch.sh
└── systemd/                       # SystemD service files for production deployment
    ├── hub/
    │   ├── kafka-hub.service
    │   ├── kafka-connect-hub.service
    │   ├── conflict-resolver.service
    │   ├── monitoring-api.service
    │   ├── schema-registry-hub.service
    │   ├── prometheus.service
    │   └── grafana.service
    └── branch/
        ├── kafka-branch.service
        ├── kafka-connect-branch.service
        ├── mirrormaker2-branch.service
        └── schema-registry-branch.service
```

## 5. Prerequisites

Before you begin, ensure you have the following installed and configured:

-   **Docker & Docker Compose**: Essential for running the Kafka, Debezium, and Spring Boot services.
-   **Oracle Database 19c**: Both for the Hub and Branch instances. Ensure they are accessible from where you run the Docker containers.
-   **SQLPlus**: Oracle SQL*Plus client for running Oracle setup scripts.
-   **Java Development Kit (JDK) 17 or higher**: Required for building Spring Boot applications.
-   **Maven**: Build tool for Spring Boot applications.
-   **Node.js and npm/yarn**: For building the React dashboard.
-   **Git**: For cloning the repository and managing code.
-   **`jq`**: Command-line JSON processor (used in shell scripts).

## 6. Quick Start Guide

### Hub Setup

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/aimanabsi/oracle-sync-general.git
    cd oracle-sync-general
    ```
2.  **Configure Environment Variables**: Copy `.env.example` to `.env` and update the Oracle connection details for your Hub database. Ensure `ORACLE_HUB_SYNC_USER` is set to `C##DBZUSER` and `ORACLE_HUB_WRITER_USER` is set to `SYNC_WRITER`.
    ```bash
    cp .env.example .env
    # Edit .env with your Oracle Hub details
    ```
3.  **Build Spring Boot Applications**: Navigate to `conflict-resolver` and `monitoring-api` directories and build them.
    ```bash
    cd conflict-resolver
    mvn clean install
    cd ../monitoring-api
    mvn clean install
    cd ..
    ```
4.  **Run Hub Setup Script**: This script will configure Oracle, start Docker services (Kafka, Debezium Connect, Schema Registry, Conflict Resolver, Monitoring API, Prometheus, Grafana), and register Debezium connectors.
    ```bash
    ./scripts/setup-hub.sh
    ```
5.  **Verify Hub Services**: After the script completes, run the health check.
    ```bash
    ./scripts/health-check.sh
    ```
    You should see all Hub services reported as healthy. You can access Kafka UI for the Hub at `http://localhost:8180`.

### Branch Setup

Repeat these steps for each branch server you want to set up.

1.  **Clone the repository** (if not already done on the branch machine):
    ```bash
    git clone https://github.com/aimanabsi/oracle-sync-general.git
    cd oracle-sync-general
    ```
2.  **Configure Environment Variables**: Copy `.env.example` to `.env` and update the Oracle connection details for your Branch database. Ensure `ORACLE_BRANCH_SYNC_USER` is set to `C##DBZUSER`, `ORACLE_BRANCH_WRITER_USER` is set to `SYNC_WRITER`, and `KAFKA_HUB_BOOTSTRAP_SERVERS` points to your Hub Kafka instance.
    ```bash
    cp .env.example .env
    # Edit .env with your Oracle Branch details and Hub Kafka details
    ```
3.  **Build Spring Boot Applications**: (Only if you are running Conflict Resolver or Monitoring API on the branch, which is not the default setup for branches in this guide. For MirrorMaker2, a Dockerfile is provided).
4.  **Run Branch Setup Script**: This script will configure Oracle, start Docker services (Kafka, Debezium Connect, Schema Registry, MirrorMaker2), and register Debezium connectors.
    ```bash
    ./scripts/setup-branch.sh
    ```
5.  **Verify Branch Services**: After the script completes, run the health check.
    ```bash
    ./scripts/health-check.sh
    ```
    You should see all Branch services reported as healthy. You can access Kafka UI for the Branch at `http://localhost:8180`.

## 7. Detailed Component Guides

### Oracle Database Configuration

Both Hub and Branch Oracle databases require specific configurations for Debezium CDC to function correctly.

-   **Enable Archive Log Mode**: Oracle must be in `ARCHIVELOG` mode.
-   **Enable Supplemental Logging**: Full supplemental logging is required to capture all necessary change information.
-   **Create Debezium User (`C##DBZUSER`)**: A dedicated Oracle user (`C##DBZUSER`) with specific privileges is needed for Debezium to access the transaction logs and tables.
-   **Create Sink Writer User (`SYNC_WRITER`)**: A dedicated Oracle user (`SYNC_WRITER`) with DML privileges on the synchronized schemas (`Z12026`, `ZLAB`) is used by the JDBC Sink Connector to apply changes. This user is configured with `DEFAULT TABLESPACE LOGMINER_TBS` and `QUOTA UNLIMITED ON LOGMINER_TBS`.

Refer to the SQL scripts in `scripts/oracle/hub/` and `scripts/oracle/branch/`. Note that the `04-conflict-columns.sql` script has been removed as conflict resolution is now handled non-intrusively by the Conflict Resolver service.

### Kafka and Debezium Connectors

Kafka is the backbone of the synchronization system. Debezium Kafka Connectors are used to capture changes from Oracle and apply them to Oracle.

-   **Kafka Broker**: Docker Compose files (`docker/hub/docker-compose-hub.yml` and `docker/branch/docker-compose-branch.yml`) define Kafka brokers with KRaft mode.
-   **Schema Registry**: Confluent Schema Registry is used to manage Avro schemas for Kafka messages, ensuring data compatibility.
-   **Debezium Oracle Source Connector**: Configured to capture changes from specified Oracle schemas (`Z12026`, `ZLAB`) using `C##DBZUSER`. Configuration files are in `connectors/hub/oracle-source-hub.json` and `connectors/branch/oracle-source-branch.json`.
-   **Debezium Oracle Sink Connector**: Configured to apply changes from Kafka topics to the Oracle database using `SYNC_WRITER`. Configuration files are in `connectors/hub/oracle-sink-hub.json` and `connectors/branch/oracle-sink-branch.json`.

### MirrorMaker2 Configuration

MirrorMaker2 (MM2) is crucial for bidirectional replication between the Hub and Branch Kafka clusters.

-   **Configuration**: MM2 is configured using properties files (`connectors/mirrormaker/mm2-hub-to-branch.properties` and `connectors/mirrormaker/mm2-branch-to-hub.properties`). These define which topics to replicate and between which clusters.
-   **Deployment**: MM2 runs as a Kafka Connect cluster itself, consuming from one cluster and producing to another.

### Conflict Resolver Service

The `conflict-resolver` is a Spring Boot application responsible for detecting and resolving data conflicts.

-   **Strategy**: Implements a Last-Write-Wins (LWW) strategy based on the Oracle SCN. If SCNs are equal, commit timestamps are used as a tie-breaker. This approach is non-intrusive and does not require altering source tables.
-   **Kafka Listener**: Consumes change events from Kafka topics (e.g., `oracle-sync.transactional.z12026`, `hub.BRANCH_1_oracle.transactional.z12026`).
-   **Kafka Producer**: Publishes resolved events to dedicated resolved topics, which are then consumed by the Oracle Sink Connectors.
-   **State Management**: Uses an in-memory map (`latestEvents`) to track the latest state of records for conflict detection. For production, this would typically be backed by a persistent state store (e.g., RocksDB, a dedicated database).

### Monitoring API

The `monitoring-api` is a Spring Boot application that provides endpoints for the dashboard to fetch real-time status and metrics.

-   **Branch Status**: Provides information about the connectivity and health of registered branches.
-   **Sync Status**: Offers high-level synchronization status.
-   **Extensibility**: Can be extended to expose more detailed metrics, conflict logs, and system health information.

### Monitoring Dashboard

The `dashboard` is a React-based web application that provides a user-friendly interface for monitoring the synchronization system.

-   **Real-time Updates**: Fetches data from the `monitoring-api` to display real-time branch connectivity status, synchronization lag, and a log of resolved conflicts.
-   **Visualizations**: Can be enhanced with charts and graphs (e.g., using Chart.js or D3.js) to visualize data flow, lag trends, and conflict rates.

### SystemD Services

SystemD service files are provided in the `systemd/` directory to manage the Docker Compose services as background processes on Linux systems. This ensures that the services start automatically on boot and restart in case of failures.

-   **Installation**: Copy the relevant `.service` files to `/etc/systemd/system/` on your Linux server.
-   **Enable and Start**: Use `sudo systemctl enable <service-name>` and `sudo systemctl start <service-name>` to manage the services.

### Kafka UI

Kafka UI is a web-based user interface for managing and monitoring Apache Kafka clusters. It provides a user-friendly way to inspect topics, view messages, manage consumers, and monitor Kafka Connect instances.

-   **Access**: Kafka UI for both Hub and Branch environments will be accessible at `http://localhost:8180` after the Docker services are started.
-   **Features**: Provides insights into Kafka topics, partitions, consumer groups, and allows for basic message browsing and production.

## 8. Troubleshooting

-   **Docker Container Issues**: If containers are not starting, check `docker logs <container_name>` for errors. Ensure Docker is running and there are no port conflicts.
-   **Oracle Connectivity**: Verify Oracle database is running and accessible from the Docker containers. Check firewall rules and Oracle listener status. Ensure the `C##DBZUSER` and `SYNC_WRITER` users have the necessary privileges.
-   **Kafka Connectors Not Registering**: Check Kafka Connect logs for errors. Ensure the Kafka broker and Schema Registry are healthy. Verify the JSON connector configurations are valid.
-   **Data Not Syncing**: Check Debezium source connector logs for CDC errors. Verify MirrorMaker2 logs for replication issues. Check Conflict Resolver logs for processing errors.
-   **Conflict Resolution Logic**: If conflicts are not resolving as expected, review the `ConflictResolverService.java` logic and ensure SCNs and timestamps are being correctly captured and compared.
-   **Monitoring Dashboard Blank**: Ensure the `monitoring-api` is running and accessible. Check browser console for API call errors.

## 9. References

-   [Debezium Documentation](https://debezium.io/documentation/)
-   [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
-   [Spring Boot Documentation](https://spring.io/projects/spring-boot)
-   [Oracle Database Documentation](https://docs.oracle.com/en/database/oracle/oracle-database/19/index.html)
-   [Docker Documentation](https://docs.docker.com/)
