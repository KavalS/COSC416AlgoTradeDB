#!/usr/bin/env bash
# =============================================================================
# pg_backup.sh — Daily PostgreSQL backup script
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration (HARDCODED — NOT RECOMMENDED FOR PUBLIC REPOS)
# ---------------------------------------------------------------------------
DB_NAME="dolcy20_db"
DB_USER="dolcy20"
DB_HOST="127.0.0.1"
DB_PORT="15433"
DB_PASSWORD=""

BACKUP_DIR="/var/backups/postgresql"
RETENTION_DAYS=14
LOG_FILE="/var/log/pg_backup.log"

ALERT_WEBHOOK=""
ALERT_EMAIL=""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
timestamp() { date +"%Y-%m-%d %H:%M:%S"; }
log()       { echo "[$(timestamp)] $*" | tee -a "$LOG_FILE"; }
die()       { log "ERROR: $*"; send_alert "$*"; exit 1; }

send_alert() {
    local msg="$1"
    if [[ -n "$ALERT_EMAIL" ]]; then
        echo "$msg" | mail -s "[pg_backup] FAILED — ${DB_NAME}@${DB_HOST}" "$ALERT_EMAIL" 2>/dev/null || true
    fi
    if [[ -n "$ALERT_WEBHOOK" ]]; then
        curl -s -X POST "$ALERT_WEBHOOK" \
            -H "Content-Type: application/json" \
            -d "{\"text\":\"pg_backup FAILED for ${DB_NAME}@${DB_HOST}: ${msg}\"}" \
            2>/dev/null || true
    fi
}

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------
for cmd in pg_dump gzip; do
    command -v "$cmd" &>/dev/null || die "'$cmd' not found — install postgresql-client."
done

mkdir -p "$BACKUP_DIR" || die "Cannot create backup directory: $BACKUP_DIR"

# ---------------------------------------------------------------------------
# Export password for pg_dump
# ---------------------------------------------------------------------------
export PGPASSWORD="$DB_PASSWORD"

# ---------------------------------------------------------------------------
# Run backup
# ---------------------------------------------------------------------------
BACKUP_FILE="${BACKUP_DIR}/${DB_NAME}_$(date +"%Y%m%d_%H%M%S").sql.gz"

log "Starting backup: ${DB_NAME} → ${BACKUP_FILE}"

pg_dump \
    --host="$DB_HOST" \
    --port="$DB_PORT" \
    --username="$DB_USER" \
    --format=plain \
    --no-password \
    "$DB_NAME" \
    | gzip -9 > "$BACKUP_FILE" \
    || die "pg_dump failed for database '${DB_NAME}'."

BACKUP_SIZE=$(du -sh "$BACKUP_FILE" | cut -f1)
log "Backup complete. Size: ${BACKUP_SIZE}  File: ${BACKUP_FILE}"

# ---------------------------------------------------------------------------
# Verify the backup
# ---------------------------------------------------------------------------
gzip -t "$BACKUP_FILE" || die "Backup file is corrupt: ${BACKUP_FILE}"
log "Integrity check passed."

# ---------------------------------------------------------------------------
# Prune old backups
# ---------------------------------------------------------------------------
log "Pruning backups older than ${RETENTION_DAYS} days…"
find "$BACKUP_DIR" \
    -maxdepth 1 \
    -name "${DB_NAME}_*.sql.gz" \
    -mtime +"$RETENTION_DAYS" \
    -print \
    -delete \
    | while read -r f; do log "Deleted old backup: $f"; done

log "Done."

unset PGPASSWORD