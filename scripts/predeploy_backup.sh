#!/usr/bin/env bash
# Create a no-clobber PostgreSQL dump and prove it restores before deploy.
set -euo pipefail
umask 077

require() { [[ -n "${!1:-}" ]] || { echo "missing required environment variable: $1" >&2; exit 2; }; }
require APP_ENV_FILE
require BACKUP_DIR
require DEPLOY_SHA
require CI_RUN_ID
require RESTORE_POSTGRES_IMAGE
require TRACKER_IMAGE
[[ "$DEPLOY_SHA" =~ ^[0-9a-f]{40}$ && "$CI_RUN_ID" =~ ^[0-9]+$ ]]
[[ -f "$APP_ENV_FILE" && ! -L "$APP_ENV_FILE" ]]
[[ -d "$BACKUP_DIR" && ! -L "$BACKUP_DIR" ]]

compose=(docker compose --env-file "$APP_ENV_FILE")
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
bundle="$BACKUP_DIR/${timestamp}-${DEPLOY_SHA}-${CI_RUN_ID}"
mkdir "$bundle"
restore_name="ft-restore-${timestamp}-${CI_RUN_ID}-$$"
restore_db="$restore_name-db"
restore_migrate="$restore_name-migrate"
restore_volume="$restore_name-data"
restore_network="$restore_name-net"

cleanup() {
    local status=$? container
    trap - EXIT
    trap '' INT TERM
    for container in "$restore_migrate" "$restore_db"; do
        if docker container inspect "$container" >/dev/null 2>&1; then
            docker rm -f "$container" >/dev/null || status=1
        fi
    done
    if docker volume inspect "$restore_volume" >/dev/null 2>&1; then
        docker volume rm "$restore_volume" >/dev/null || status=1
    fi
    if docker network inspect "$restore_network" >/dev/null 2>&1; then
        docker network rm "$restore_network" >/dev/null || status=1
    fi
    if (( status != 0 )); then
        rm -rf -- "$bundle"
    else
        printf 'PREDEPLOY_BACKUP bundle=%s restore_drill=passed\n' "$bundle"
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

database_name="$("${compose[@]}" exec -T db sh -eu -c 'printf %s "$POSTGRES_DB"')"
database_user="$("${compose[@]}" exec -T db sh -eu -c 'printf %s "$POSTGRES_USER"')"
[[ "$database_name" =~ ^[A-Za-z0-9_]+$ && "$database_user" =~ ^[A-Za-z0-9_]+$ ]]

dump_path="$bundle/postgres.dump"
"${compose[@]}" exec -T db sh -eu -c 'pg_dump --format=custom --no-owner --no-privileges -U "$POSTGRES_USER" -d "$POSTGRES_DB"' > "$dump_path"
test -s "$dump_path"
"${compose[@]}" exec -T db psql -X -q -A -t -U "$database_user" -d "$database_name" -c "SELECT filename || ' ' || checksum_sha256 FROM schema_migrations ORDER BY filename" > "$bundle/schema_migrations.txt"
sha256sum "$dump_path" "$bundle/schema_migrations.txt" > "$bundle/SHA256SUMS"

# The source instance is used only for the reads above. Keep production
# environment, volumes and networks out of the restore containers.
POSTGRES_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
export POSTGRES_PASSWORD
export DB_PASSWORD="$POSTGRES_PASSWORD"
docker network create --internal "$restore_network" >/dev/null
docker volume create "$restore_volume" >/dev/null
docker run -d --pull=never --name "$restore_db" --network "$restore_network" \
    --mount "type=volume,src=$restore_volume,dst=/var/lib/postgresql/data" \
    -e POSTGRES_PASSWORD -e POSTGRES_DB=restore_check "$RESTORE_POSTGRES_IMAGE" >/dev/null

timeout --kill-after=5s 60s bash -c '
    until docker exec "$1" pg_isready -q -h 127.0.0.1 -U postgres -d restore_check; do
        sleep 1
    done
' bash "$restore_db"
timeout --kill-after=5s 300s docker exec -i "$restore_db" \
    pg_restore --exit-on-error --no-owner --no-privileges -U postgres -d restore_check < "$dump_path"

restore=(docker run --rm --pull=never --name "$restore_migrate" --network "$restore_network"
    --read-only --cap-drop=ALL --security-opt=no-new-privileges
    --tmpfs /tmp:rw,noexec,nosuid,size=64m
    -e "DB_HOST=$restore_db" -e DB_NAME=restore_check -e DB_USER=postgres -e DB_PASSWORD
    -e MIGRATIONS_DIR=/app/migrations --entrypoint financetracker-migrate "$TRACKER_IMAGE")
timeout --kill-after=5s 300s "${restore[@]}"
timeout --kill-after=5s 300s "${restore[@]}" --check

printf '{"source_sha":"%s","ci_run_id":"%s","database":"%s","dump":"postgres.dump","schema_ledger":"schema_migrations.txt","restore_drill":"passed"}\n' "$DEPLOY_SHA" "$CI_RUN_ID" "$database_name" > "$bundle/manifest.json"
sha256sum "$bundle/manifest.json" >> "$bundle/SHA256SUMS"
