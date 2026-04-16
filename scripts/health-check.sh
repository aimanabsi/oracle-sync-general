#!/bin/bash

# Comprehensive Health Check for Oracle Sync General Services

set -e

# Color codes for output
RED=\'\\033[0;31m\'
GREEN=\'\\033[0;32m\'
YELLOW=\'\\033[1;33m\'
NC=\'\\033[0m\' # No Color

echo -e "${YELLOW}Starting comprehensive health check...${NC}"

# Function to check service health
check_service() {
    SERVICE_NAME=$1
    URL=$2
    EXPECTED_STATUS=$3
    MESSAGE=$4

    echo -e "${YELLOW}Checking $SERVICE_NAME at $URL...${NC}"
    STATUS_CODE=$(curl -s -o /dev/null -w "%{http_code}" $URL || echo "000")

    if [ "$STATUS_CODE" = "$EXPECTED_STATUS" ]; then
        echo -e "${GREEN}✓ $SERVICE_NAME is healthy ($MESSAGE)${NC}"
    else
        echo -e "${RED}✗ $SERVICE_NAME is unhealthy (HTTP Status: $STATUS_CODE). Expected: $EXPECTED_STATUS${NC}"
        exit 1
    fi
}

# Check Hub Services
echo -e "\n${YELLOW}--- Checking Hub Services ---${NC}"
check_service "Kafka Connect Hub" "http://localhost:8083/connectors" "200" "Connectors API reachable"
check_service "Schema Registry Hub" "http://localhost:8081/subjects" "200" "Subjects API reachable"
check_service "Conflict Resolver" "http://localhost:8080/actuator/health" "200" "Health endpoint reachable"
check_service "Monitoring API" "http://localhost:8088/actuator/health" "200" "Health endpoint reachable"
check_service "Prometheus" "http://localhost:9090/graph" "200" "Prometheus UI reachable"
check_service "Grafana" "http://localhost:3000/login" "200" "Grafana login page reachable"
check_service "Kafka UI Hub" "http://localhost:8180" "200" "Kafka UI reachable"

# Check Branch Services (assuming a branch is running on localhost for testing)
echo -e "\n${YELLOW}--- Checking Branch Services (assuming local branch) ---${NC}"
check_service "Kafka Connect Branch" "http://localhost:8084/connectors" "200" "Connectors API reachable"
check_service "Schema Registry Branch" "http://localhost:8082/subjects" "200" "Subjects API reachable"
check_service "Kafka UI Branch" "http://localhost:8180" "200" "Kafka UI reachable"

echo -e "\n${GREEN}All essential services are healthy!${NC}"
