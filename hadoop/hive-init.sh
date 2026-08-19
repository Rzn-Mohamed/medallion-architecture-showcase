#!/bin/bash
# hive-init.sh — run inside hive-metastore container
# Initialises the Postgres schema on first boot; safe to re-run (errors are ignored).
set -e

echo "[hive-init] Running schematool -initSchema ..."
/opt/hive/bin/schematool -dbType postgres -initSchema 2>&1 || {
    echo "[hive-init] schematool returned non-zero (schema may already exist) — continuing"
}

echo "[hive-init] Starting Hive Metastore service ..."
exec /opt/hive/bin/hive --service metastore
