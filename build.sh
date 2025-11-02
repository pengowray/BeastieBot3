#!/usr/bin/env bash
set -euo pipefail

# Ensure we run from the repo root (this script's directory)
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
cd "$SCRIPT_DIR"

echo "Building .NET project..."
dotnet build BeastieBot3.sln

# Pick Docker Compose command (v2 plugin preferred)
if docker compose version >/dev/null 2>&1; then
 COMPOSE=(docker compose)
elif docker-compose version >/dev/null 2>&1; then
 COMPOSE=(docker-compose)
else
 echo "ERROR: Docker Compose not found. Install Docker Desktop or docker-compose." >&2
 exit 1
fi

if [[ "${1:-}" == "rebuild" ]]; then
 echo "Rebuilding image with no cache and pulling latest base layers..."
 "${COMPOSE[@]}" build --no-cache --pull
else
 echo "Building image (cached)..."
 "${COMPOSE[@]}" build
fi
