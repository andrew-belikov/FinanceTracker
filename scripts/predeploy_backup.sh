#!/usr/bin/env bash
# Create a no-clobber PostgreSQL dump and prove it restores before deploy.
set -euo pipefail
umask 077

require() { [[ -n "${!1:-}" ]] || { echo "missing required environment variable: $1" >&2; exit 2; }; }
require APP_ENV_FILE
require BACKUP_DIR
require DEPLOY_SHA
require CI_RUN_ID
[[ "$DEPLOY_SHA" =~ ^[0-9a-f]{40}$ && "$CI_RUN_ID" =~ ^[0-9]+$ ]]
[[ -f "$APP_ENV_FILE" && ! -L "$APP_ENV_FILE" ]]
[[ -d "$BACKUP_DIR" && ! -L "$BACKUP_DIR" ]]

compose=(docker compose --env-file "$APP_ENV_FILE")
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
bundle="$BACKUP_DIR/${timestamp}-${DEPLOY_SHA}-${CI_RUN_ID}"
mkdir "$bundle"
trap 'rm -rf -- "$bundle"' ERR

database_name="$(${compose[@]} exec -T db sh -eu -c 'printf %s "$POSTGRES_DB"')"
database_user="$(${compose[@]} exec -T db sh -eu -c 'printf %s "$POSTGRES_USER"')"
[[ "$database_name" =~ ^[A-Za-z0-9_]+$ && "$database_user" =~ ^[A-Za-z0-9_]+$ ]]

dump_path="$bundle/postgres.dump"
${compose[@]} exec -T db sh -eu -c 'pg_dump --format=custom --no-owner --no-privileges -U "$POSTGRES_USER" -d "$POSTGRES_DB"' > "$dump_path"
test -s "$dump_path"
${compose[@]} exec -T db psql -X -q -A -t -U "$database_user" -d "$database_name" -c "SELECT filename || ' ' || checksum_sha256 FROM schema_migrations ORDER BY filename" > "$bundle/schema_migrations.txt"
sha256sum "$dump_path" "$bundle/schema_migrations.txt" > "$bundle/SHA256SUMS"

restore_database="financetracker_restore_${CI_RUN_ID}_$$"
cleanup_restore() { ${compose[@]} exec -T db dropdb -U "$database_user" --if-exists "$restore_database" >/dev/null 2>&1 || true; }
trap 'cleanup_restore; rm -rf -- "$bundle"' ERR INT TERM
${compose[@]} exec -T db createdb -U "$database_user" "$restore_database"
${compose[@]} exec -T db pg_restore -U "$database_user" -d "$restore_database" < "$dump_path"
POSTGRES_DB="$restore_database" ${compose[@]} run --rm --no-deps migrate --check
cleanup_restore
trap - ERR INT TERM

printf '{"source_sha":"%s","ci_run_id":"%s","database":"%s","dump":"postgres.dump","schema_ledger":"schema_migrations.txt","restore_drill":"passed"}\n' "$DEPLOY_SHA" "$CI_RUN_ID" "$database_name" > "$bundle/manifest.json"
sha256sum "$bundle/manifest.json" >> "$bundle/SHA256SUMS"
printf 'PREDEPLOY_BACKUP bundle=%s restore_drill=passed\n' "$bundle"
