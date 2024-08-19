#!/bin/bash
#CURRENTLY MANUAL SETTING THIS WITH CLI, THIS IS A DRAFT AUTOMATION SCRIPT


# swarm mode
if ! docker info | grep -q "Swarm: active"; then
    echo "Initializing Docker Swarm..."
    docker swarm init
fi

# Read API key from .env file
if [ -f .env ]; then
    API_KEY=$(grep CDISC_LIBRARY_API_KEY .env | cut -d '=' -f2)
    if [ -z "$API_KEY" ]; then
        echo "Error: CDISC_LIBRARY_API_KEY not found in .env file"
        exit 1
    fi
else
    echo "Error: .env file not found"
    exit 1
fi

echo "Creating Docker secret..."
echo "$API_KEY" | docker secret create cdisc_api_key - 2>/dev/null || \
    (docker secret rm cdisc_api_key && echo "$API_KEY" | docker secret create cdisc_api_key -)

echo "Building Docker image..."
docker build -t cdisc-rules-engine .

# Deploy/update
echo "Deploying the service..."
if docker service ls | grep -q cdisc-rules-engine; then
    docker service update \
        --secret-rm cdisc_api_key \
        --secret-add cdisc_api_key \
        --force \
        --mount type=bind,source=${PWD},destination=/app/output `
        cdisc-rules-engine
else
    docker service create \
        --name cdisc-rules-engine \
        --secret cdisc_api_key \
            --mount type=bind,source=${PWD},destination=/app/output ` \
        cdisc-rules-engine
fi

echo "Service logs:"
docker service logs -f cdisc-rules-engine


#when job ends, remove the service
if 
    docker service rm cdisc-rules-engine