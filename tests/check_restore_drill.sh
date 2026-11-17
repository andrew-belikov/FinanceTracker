#!/usr/bin/env bash
# Run against the synthetic Compose smoke stack with its migrations applied.
set -euo pipefail
[[ "${COMPOSE_FILE:-}" == compose.yml:compose.ci.yml ]]
compose=(docker compose --env-file "${APP_ENV_FILE:?}")
source_id="$("${compose[@]}" ps -q db)"
[[ "$(docker inspect -f '{{.Name}}' "$source_id")" == /financetracker-ci-db-1 ]]

scratch="$(mktemp -d)"
trap 'rm -rf -- "$scratch"' EXIT
export BACKUP_DIR="$scratch/backups"
mkdir "$BACKUP_DIR" "$scratch/bin"
export DEPLOY_SHA="$(git rev-parse HEAD)"
export CI_RUN_ID="99$$"
export RESTORE_POSTGRES_IMAGE
RESTORE_POSTGRES_IMAGE="$(docker inspect -f '{{.Image}}' "$source_id")"
export TRACKER_IMAGE="${TRACKER_IMAGE:-financetracker-tracker}"

source_databases() {
    "${compose[@]}" exec -T db sh -ec '
        psql -XAt -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
            -c "SELECT datname FROM pg_database ORDER BY datname"
    '
}
assert_cleanup() {
    local names
    names="$(docker ps -a --format '{{.Names}}')"
    [[ "$names" != *"-$CI_RUN_ID-"* ]]
    names="$(docker volume ls --format '{{.Name}}')"
    [[ "$names" != *"-$CI_RUN_ID-"* ]]
    names="$(docker network ls --format '{{.Name}}')"
    [[ "$names" != *"-$CI_RUN_ID-"* ]]
}

source_before="$(source_databases)"
./scripts/predeploy_backup.sh
bundles=("$BACKUP_DIR"/*)
[[ "${#bundles[@]}" == 1 ]]
sha256sum --check "${bundles[0]}/SHA256SUMS"
grep -q '"restore_drill":"passed"' "${bundles[0]}/manifest.json"
assert_cleanup

export REAL_DOCKER="$(command -v docker)"
export RESTORE_FAILURE_MARKER="$scratch/restore-failure"
cat > "$scratch/bin/docker" <<'WRAPPER'
#!/usr/bin/env bash
set -eu
if [[ "${1:-}" == exec && "${2:-}" == -i && "${4:-}" == pg_restore ]]; then
    [[ "$3" == ft-restore-*-db ]]
    if [[ -n "${RESTORE_SCRIPT_PID:-}" ]]; then
        kill -TERM "$RESTORE_SCRIPT_PID"
        exit 0
    fi
    touch "$RESTORE_FAILURE_MARKER"
    printf 'corrupted archive' | "$REAL_DOCKER" "$@"
    exit "$?"
fi
exec "$REAL_DOCKER" "$@"
WRAPPER
chmod +x "$scratch/bin/docker"
export CI_RUN_ID="98$$"
if PATH="$scratch/bin:$PATH" ./scripts/predeploy_backup.sh; then
    echo 'Restore drill accepted a corrupt backup' >&2
    exit 1
fi
test -f "$RESTORE_FAILURE_MARKER"
assert_cleanup

export CI_RUN_ID="97$$"
cancel_status=0
PATH="$scratch/bin:$PATH" bash -c 'export RESTORE_SCRIPT_PID=$$; exec ./scripts/predeploy_backup.sh' || cancel_status=$?
[[ "$cancel_status" == 143 ]]
assert_cleanup

[[ -z "$(find "$BACKUP_DIR" -mindepth 1 -maxdepth 1 ! -path "${bundles[0]}" -print)" ]]
[[ "$("${compose[@]}" ps -q db)" == "$source_id" ]]
[[ "$(source_databases)" == "$source_before" ]]
printf 'Restore drill passed success, corrupt archive and cancellation scenarios.\n'
