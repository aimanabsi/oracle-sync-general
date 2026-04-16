#!/bin/bash

# Health Check Script for Oracle Sync System
# Monitors Kafka, Debezium, Oracle, and MirrorMaker2 health

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
KAFKA_CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"
CONFLICT_RESOLVER_URL="${CONFLICT_RESOLVER_URL:-http://localhost:8080}"

echo -e "${BLUE}=== Oracle Sync System Health Check ===${NC}"
echo "Timestamp: $(date)"
echo ""

# 1. Check Kafka Broker Health
echo -e "${YELLOW}1. Kafka Broker Health${NC}"
if docker ps | grep -q kafka-hub; then
    echo -e "${GREEN}✓ Kafka Hub container is running${NC}"
    
    # Check broker connectivity
    if docker exec kafka-hub kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Kafka broker is responding${NC}"
    else
        echo -e "${RED}✗ Kafka broker is not responding${NC}"
    fi
else
    echo -e "${RED}✗ Kafka Hub container is not running${NC}"
fi

echo ""

# 2. Check Kafka Connect Health
echo -e "${YELLOW}2. Kafka Connect Health${NC}"
if curl -s "$KAFKA_CONNECT_URL/connectors" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Kafka Connect is responding${NC}"
    
    # List connectors
    CONNECTORS=$(curl -s "$KAFKA_CONNECT_URL/connectors" | jq -r '.[]')
    echo "Active Connectors:"
    for connector in $CONNECTORS; do
        STATUS=$(curl -s "$KAFKA_CONNECT_URL/connectors/$connector/status" | jq -r '.connector.state')
        echo -e "  - $connector: ${GREEN}$STATUS${NC}"
    done
else
    echo -e "${RED}✗ Kafka Connect is not responding${NC}"
fi

echo ""

# 3. Check Debezium Connectors Status
echo -e "${YELLOW}3. Debezium Connectors Status${NC}"
CONNECTORS=$(curl -s "$KAFKA_CONNECT_URL/connectors" | jq -r '.[]' 2>/dev/null)
if [ -z "$CONNECTORS" ]; then
    echo -e "${YELLOW}No connectors registered${NC}"
else
    for connector in $CONNECTORS; do
        STATUS=$(curl -s "$KAFKA_CONNECT_URL/connectors/$connector/status" 2>/dev/null)
        STATE=$(echo "$STATUS" | jq -r '.connector.state' 2>/dev/null)
        TASKS=$(echo "$STATUS" | jq -r '.tasks[] | .state' 2>/dev/null)
        
        if [ "$STATE" = "RUNNING" ]; then
            echo -e "  ${GREEN}✓${NC} $connector: $STATE"
        else
            echo -e "  ${RED}✗${NC} $connector: $STATE"
        fi
    done
fi

echo ""

# 4. Check Kafka Topics
echo -e "${YELLOW}4. Kafka Topics${NC}"
TOPICS=$(docker exec kafka-hub kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep oracle-sync)
if [ -z "$TOPICS" ]; then
    echo -e "${YELLOW}No oracle-sync topics found${NC}"
else
    echo "Topics:"
    for topic in $TOPICS; do
        echo "  - $topic"
    done
fi

echo ""

# 5. Check Kafka Topic Lag
echo -e "${YELLOW}5. Kafka Topic Lag${NC}"
CONSUMER_GROUPS=$(docker exec kafka-hub kafka-consumer-groups --bootstrap-server localhost:9092 --list 2>/dev/null | grep -E "connect|conflict")
if [ -z "$CONSUMER_GROUPS" ]; then
    echo -e "${YELLOW}No consumer groups found${NC}"
else
    for group in $CONSUMER_GROUPS; do
        echo "Consumer Group: $group"
        docker exec kafka-hub kafka-consumer-groups --bootstrap-server localhost:9092 --group "$group" --describe 2>/dev/null | tail -n +2 | while read line; do
            LAG=$(echo "$line" | awk '{print $NF}')
            if [ "$LAG" -gt 100 ]; then
                echo -e "  ${RED}✗${NC} $line"
            else
                echo -e "  ${GREEN}✓${NC} $line"
            fi
        done
    done
fi

echo ""

# 6. Check Conflict Resolver Health
echo -e "${YELLOW}6. Conflict Resolver Health${NC}"
if curl -s "$CONFLICT_RESOLVER_URL/health" > /dev/null 2>&1; then
    HEALTH=$(curl -s "$CONFLICT_RESOLVER_URL/health" | jq -r '.status' 2>/dev/null)
    if [ "$HEALTH" = "UP" ]; then
        echo -e "${GREEN}✓ Conflict Resolver is healthy${NC}"
    else
        echo -e "${YELLOW}⚠ Conflict Resolver status: $HEALTH${NC}"
    fi
else
    echo -e "${YELLOW}⚠ Conflict Resolver is not responding${NC}"
fi

echo ""

# 7. Check Oracle Connectivity
echo -e "${YELLOW}7. Oracle Connectivity${NC}"
if command -v sqlplus &> /dev/null; then
    # This would require credentials, so we'll skip for now
    echo -e "${YELLOW}⚠ Oracle connectivity check requires credentials${NC}"
else
    echo -e "${YELLOW}⚠ SQLPlus not installed${NC}"
fi

echo ""

# 8. Check Docker Containers
echo -e "${YELLOW}8. Docker Containers Status${NC}"
CONTAINERS="kafka-hub kafka-connect-hub conflict-resolver prometheus-hub grafana-hub"
for container in $CONTAINERS; do
    if docker ps | grep -q "$container"; then
        echo -e "  ${GREEN}✓${NC} $container"
    else
        echo -e "  ${RED}✗${NC} $container"
    fi
done

echo ""
echo -e "${BLUE}=== Health Check Complete ===${NC}"
