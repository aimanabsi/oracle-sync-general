#!/bin/bash

# Setup Hub Server for Oracle Sync
# This script configures the hub server with Kafka, Debezium, and Oracle

set -e

# Color codes for output
RED=\'\\033[0;31m\'
GREEN=\'\\033[0;32m\'
YELLOW=\'\\033[1;33m\'
NC=\'\\033[0m\' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"
DOCKER_DIR="$PROJECT_DIR/docker/hub"
CONNECTORS_DIR="$PROJECT_DIR/connectors/hub"

echo -e "${YELLOW}Starting Hub Server Setup...${NC}"

# 1. Load environment variables
if [ -f "$PROJECT_DIR/.env" ]; then
    source "$PROJECT_DIR/.env"
    echo -e "${GREEN}✓ Loaded environment variables${NC}"
else
    echo -e "${RED}✗ .env file not found. Please copy .env.example to .env and configure it.${NC}"
    exit 1
fi

# 2. Verify Oracle connectivity
echo -e "${YELLOW}Verifying Oracle Hub connectivity...${NC}"
sqlplus -v > /dev/null 2>&1 || {
    echo -e "${RED}✗ SQLPlus not found. Please install Oracle client.${NC}"
    exit 1
}

# 3. Run Oracle setup scripts
echo -e "${YELLOW}Running Oracle setup scripts...${NC}"

# Enable LogMiner
echo "Executing 01-enable-logminer.sql..."
sqlplus sys/"${ORACLE_HUB_SYS_PASSWORD}"@//"${ORACLE_HUB_HOST}":"${ORACLE_HUB_PORT}"/"${ORACLE_HUB_PDB}" as sysdba @"$SCRIPT_DIR/oracle/hub/01-enable-logminer.sql" || {
    echo -e "${RED}✗ Failed to enable LogMiner${NC}"
    exit 1
}
echo -e "${GREEN}✓ LogMiner enabled${NC}"

# Create sync user
echo "Executing 02-create-sync-user.sql..."
sqlplus sys/"${ORACLE_HUB_SYS_PASSWORD}"@//"${ORACLE_HUB_HOST}":"${ORACLE_HUB_PORT}"/"${ORACLE_HUB_PDB}" as sysdba @"$SCRIPT_DIR/oracle/hub/02-create-sync-user.sql" || {
    echo -e "${RED}✗ Failed to create sync user${NC}"
    exit 1
}
echo -e "${GREEN}✓ Sync user created${NC}"

# Enable supplemental logging
echo "Executing 03-supplemental-logging.sql..."
sqlplus sys/"${ORACLE_HUB_SYS_PASSWORD}"@//"${ORACLE_HUB_HOST}":"${ORACLE_HUB_PORT}"/"${ORACLE_HUB_PDB}" as sysdba @"$SCRIPT_DIR/oracle/hub/03-supplemental-logging.sql" || {
    echo -e "${RED}✗ Failed to enable supplemental logging${NC}"
    exit 1
}
echo -e "${GREEN}✓ Supplemental logging enabled${NC}"

# 4. Start Docker services
echo -e "${YELLOW}Starting Docker services...${NC}"
cd "$DOCKER_DIR"
docker-compose -f docker-compose-hub.yml up -d

# Wait for services to start
echo -e "${YELLOW}Waiting for services to start...${NC}"
sleep 45 # Increased sleep time for more services

# Check if services are running
docker-compose -f docker-compose-hub.yml ps

# 5. Register Debezium connectors
echo -e "${YELLOW}Registering Debezium connectors...${NC}"

# Wait for Kafka Connect to be ready
echo "Waiting for Kafka Connect to be ready..."
for i in {1..45}; do # Increased attempts
    if curl -s http://localhost:8083/connectors > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Kafka Connect is ready${NC}"
        break
    fi
    echo "Attempt $i/45: Waiting for Kafka Connect..."
    sleep 2
done

# Register Oracle Source Connector
echo "Registering oracle-source-hub connector..."
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @"$CONNECTORS_DIR/oracle-source-hub.json" || {
    echo -e "${RED}✗ Failed to register oracle-source-hub connector${NC}"
}
echo -e "${GREEN}✓ oracle-source-hub connector registered${NC}"

# Register Oracle Sink Connector
echo "Registering oracle-sink-hub connector..."
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @"$CONNECTORS_DIR/oracle-sink-hub.json" || {
    echo -e "${RED}✗ Failed to register oracle-sink-hub connector${NC}"
}
echo -e "${GREEN}✓ oracle-sink-hub connector registered${NC}"

# 6. Verify setup
echo -e "${YELLOW}Verifying setup...${NC}"

# Check Kafka topics
echo "Checking Kafka topics..."
docker exec kafka-hub kafka-topics --bootstrap-server localhost:9092 --list

# Check Debezium connectors
echo "Checking Debezium connectors..."
curl -s http://localhost:8083/connectors | jq "."

# Check Schema Registry
echo "Checking Schema Registry..."
curl -s http://localhost:8081/subjects || echo -e "${RED}✗ Schema Registry not reachable${NC}"

# Check Monitoring API
echo "Checking Monitoring API..."
curl -s http://localhost:8088/actuator/health || echo -e "${RED}✗ Monitoring API not reachable${NC}"

echo -e "${GREEN}✓ Hub Server Setup Complete!${NC}"
echo ""
echo "Hub Services:"
echo "  - Kafka: localhost:9092"
echo "  - Kafka Connect: http://localhost:8083"
echo "  - Schema Registry: http://localhost:8081"
echo "  - Conflict Resolver: http://localhost:8080"
echo "  - Monitoring API: http://localhost:8088"
echo "  - Prometheus: http://localhost:9090"
echo "  - Grafana: http://localhost:3000 (admin/admin)"
echo ""
echo "Next steps:"
echo "  1. Setup branch servers using ./scripts/setup-branch.sh"
echo "  2. Monitor sync progress in Grafana"
echo "  3. Check logs: docker logs <container-name>"
