#!/usr/bin/env sh
# deploy/migrate.sh
#
# Runs all pending migrations for both databases using sqlx-cli.
# Executed by the 'migrate' service in docker-compose before the application
# starts. Exits non-zero on any failure so dependent services won't start.
#
# Required env vars:
#   NOTIFICATION_DATABASE_URL — notification service DB
#   BUSINESS_DATABASE_URL     — business service DB (outbox table lives here)

set -e

echo "[migrate] Starting migration runner"

# ── Notification DB ───────────────────────────────────────────────────────────
if [ -z "$NOTIFICATION_DATABASE_URL" ]; then
  echo "[migrate] ERROR: NOTIFICATION_DATABASE_URL is not set"
  exit 1
fi

echo "[migrate] Running notification DB migrations..."
sqlx migrate run \
  --database-url "$NOTIFICATION_DATABASE_URL" \
  --source /migrations
echo "[migrate] Notification DB migrations complete"

# ── Business DB ───────────────────────────────────────────────────────────────
if [ -z "$BUSINESS_DATABASE_URL" ]; then
  echo "[migrate] BUSINESS_DATABASE_URL not set — skipping business DB migrations"
else
  echo "[migrate] Running business DB migrations..."
  sqlx migrate run \
    --database-url "$BUSINESS_DATABASE_URL" \
    --source /migrations/business_db
  echo "[migrate] Business DB migrations complete"
fi

echo "[migrate] All migrations applied successfully"
