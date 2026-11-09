#!/usr/bin/env bash
# Apply CI-built application images to an existing, backed-up production stack.
set -euo pipefail
umask 077

compose=(docker compose --env-file "${APP_ENV_FILE:?}")
services=(reporter tracker xray-client bot)
check_name="financetracker-migrate-check-${DEPLOY_SHA:?}"
declare -A previous_images image_ids

running_container() {
  "${compose[@]}" ps --quiet "$1"
}

verify_image_tag() {
  test "$(docker image inspect --format '{{.Id}}' "$1")" = "$2"
}

assert_current_main() {
  test "$(git ls-remote --exit-code origin refs/heads/main | cut -f1)" = "$DEPLOY_SHA" ||
    { echo "Refusing to deploy a revision that is no longer main" >&2; return 1; }
}

test -d "${BACKUP_DIR:?}"
test ! -L "$BACKUP_DIR"
exec 9>"$BACKUP_DIR/.deploy.lock"
flock 9
assert_current_main

verify_image_tag "$BOT_IMAGE" "$EXPECTED_BOT_IMAGE_ID"
verify_image_tag "$TRACKER_IMAGE" "$EXPECTED_TRACKER_IMAGE_ID"
verify_image_tag "$REPORTER_IMAGE" "$EXPECTED_REPORTER_IMAGE_ID"
verify_image_tag "$XRAY_CLIENT_IMAGE" "$EXPECTED_XRAY_CLIENT_IMAGE_ID"
export BOT_IMAGE="$EXPECTED_BOT_IMAGE_ID" TRACKER_IMAGE="$EXPECTED_TRACKER_IMAGE_ID"
export REPORTER_IMAGE="$EXPECTED_REPORTER_IMAGE_ID" XRAY_CLIENT_IMAGE="$EXPECTED_XRAY_CLIENT_IMAGE_ID"
image_ids=([bot]="$BOT_IMAGE" [tracker]="$TRACKER_IMAGE" [reporter]="$REPORTER_IMAGE" [xray-client]="$XRAY_CLIENT_IMAGE")

db_container_id="$(running_container db)"
test -n "$db_container_id"
db_image_ref="$("${compose[@]}" config --images db)"
db_image_id="$(docker inspect --format '{{.Image}}' "$db_container_id")"
verify_image_tag "$db_image_ref" "$db_image_id"
test "$(docker inspect --format '{{.State.Health.Status}}' "$db_container_id")" = healthy

for service in "${services[@]}"; do
  container_id="$(running_container "$service")"
  test -n "$container_id"
  previous_images[$service]="$(docker inspect --format '{{.Image}}' "$container_id")"
done

./scripts/predeploy_backup.sh
assert_current_main

if ! "${compose[@]}" run --rm --no-deps -e 'PGOPTIONS=-c default_transaction_read_only=on' migrate --check; then
  "${compose[@]}" run --rm --no-deps migrate
fi
"${compose[@]}" run --rm --no-deps -e 'PGOPTIONS=-c default_transaction_read_only=on' migrate --check

up_service() {
  "${compose[@]}" up -d --no-deps --no-build --pull never --wait --wait-timeout 180 "$1"
}

changed=()
rollback_on_failure() {
  status=$?
  trap - EXIT INT TERM
  docker rm "$check_name" >/dev/null 2>&1 || true
  if (( status != 0 && ${#changed[@]} > 0 )); then
    export BOT_IMAGE="${previous_images[bot]}" TRACKER_IMAGE="${previous_images[tracker]}"
    export REPORTER_IMAGE="${previous_images[reporter]}" XRAY_CLIENT_IMAGE="${previous_images[xray-client]}"
    for ((i=${#changed[@]}-1; i>=0; i--)); do
      service="${changed[i]}"
      if up_service "$service" && test "$(docker inspect --format '{{.Image}}' "$(running_container "$service")")" = "${previous_images[$service]}"; then
        printf 'Restored previous image for %s\n' "$service"
      else
        printf 'Could not restore previous image for %s\n' "$service" >&2
      fi
    done
  fi
  exit "$status"
}
trap rollback_on_failure EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

assert_current_main
for service in "${services[@]}"; do
  changed+=("$service")
  up_service "$service"
  container_id="$(running_container "$service")"
  test "$(docker inspect --format '{{.Image}}' "$container_id")" = "${image_ids[$service]}"
  test "$(docker inspect --format '{{.State.Health.Status}}' "$container_id")" = healthy
done

test "$(running_container db)" = "$db_container_id"
test "$(docker inspect --format '{{.State.Health.Status}}' "$db_container_id")" = healthy

"${compose[@]}" run --name "$check_name" --no-deps -e 'PGOPTIONS=-c default_transaction_read_only=on' migrate --check
check_image_id="$(docker inspect --format '{{.Image}}' "$check_name")"
test "$check_image_id" = "$TRACKER_IMAGE"
docker rm "$check_name" >/dev/null

{
  printf 'ACTUAL_BOT_IMAGE_ID=%s\n' "$BOT_IMAGE"
  printf 'ACTUAL_TRACKER_IMAGE_ID=%s\n' "$TRACKER_IMAGE"
  printf 'ACTUAL_REPORTER_IMAGE_ID=%s\n' "$REPORTER_IMAGE"
  printf 'ACTUAL_XRAY_CLIENT_IMAGE_ID=%s\n' "$XRAY_CLIENT_IMAGE"
  printf 'ACTUAL_MIGRATE_IMAGE_ID=%s\n' "$check_image_id"
  printf 'ACTUAL_DB_IMAGE_REF=%s\n' "$db_image_ref"
  printf 'ACTUAL_DB_IMAGE_ID=%s\n' "$db_image_id"
} >> "${GITHUB_ENV:?}"
trap - EXIT INT TERM
