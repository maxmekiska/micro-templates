#!/bin/bash

set -e

IMAGE_NAME="mc-producer:latest"
CONTAINER_NAME="my-mc-producer-instance"
NETWORK_NAME="confluent-kafka-rust_kafka_network"

echo "--- Building Docker image '$IMAGE_NAME' ---"
docker build -t "$IMAGE_NAME" .

echo "--- Checking for and removing existing container '$CONTAINER_NAME' ---"
if docker ps -a --format "{{.Names}}" | grep -q "^$CONTAINER_NAME$"; then
    echo "Stopping existing container '$CONTAINER_NAME'..."
    docker stop "$CONTAINER_NAME" > /dev/null 2>&1 || echo "Warning: Could not stop '$CONTAINER_NAME', it might not be running. Attempting to remove anyway."

    echo "Removing container '$CONTAINER_NAME'..."
    docker rm "$CONTAINER_NAME" > /dev/null 2>&1 || { echo "Error: Could not remove '$CONTAINER_NAME'. Please remove it manually: docker rm -f $CONTAINER_NAME"; exit 1; }
    echo "Container '$CONTAINER_NAME' removed."
else
    echo "No existing container '$CONTAINER_NAME' found."
fi

echo "--- Running new container '$CONTAINER_NAME' on network '$NETWORK_NAME' ---"
docker run --name "$CONTAINER_NAME" \
           --network "$NETWORK_NAME" \
           "$IMAGE_NAME"

echo "--- Script finished. Container '$CONTAINER_NAME' is running (or attempting to run). ---"