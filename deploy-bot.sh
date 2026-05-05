#!/usr/bin/env bash
set -e

COMPOSE="docker compose -f /opt/cinetrace/docker-compose.prod.yml"

echo "==> Pulling latest code..."
cd /opt/cinetrace
git fetch origin
git reset --hard origin/$(git rev-parse --abbrev-ref HEAD)

echo "==> Rebuilding and restarting bot..."
$COMPOSE up -d --build bot

echo "==> Waiting for container to settle..."
sleep 3

echo "==> Tailing logs (Ctrl+C to stop)..."
$COMPOSE logs -f bot
