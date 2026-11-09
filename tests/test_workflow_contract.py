import re
from pathlib import Path
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CI_TEXT = (PROJECT_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
ROLLOUT = (PROJECT_ROOT / "scripts" / "deploy_services.sh").read_text(encoding="utf-8")
DEPLOY_TEXT = (PROJECT_ROOT / ".github" / "workflows" / "deploy.yml").read_text(encoding="utf-8")
CODEQL_TEXT = (PROJECT_ROOT / ".github" / "workflows" / "codeql.yml").read_text(encoding="utf-8")


class WorkflowContractTests(unittest.TestCase):
    def test_all_actions_are_pinned_to_immutable_commit_sha(self):
        uses = re.findall(r"^\s*uses:\s*([^\s#]+)", CI_TEXT + "\n" + DEPLOY_TEXT + "\n" + CODEQL_TEXT, re.MULTILINE)
        self.assertTrue(uses)
        for action in uses:
            with self.subTest(action=action):
                self.assertRegex(action, r"^[^@]+@[0-9a-f]{40}$")

    def test_ci_installs_hash_locked_dependencies_and_runs_explicit_gates(self):
        self.assertIn("--require-hashes", CI_TEXT)
        self.assertIn("Run workflow contract checks", CI_TEXT)
        self.assertIn("Run secret scan", CI_TEXT)
        self.assertIn("python -m coverage run -m unittest", CI_TEXT)
        self.assertIn("python -m coverage report", CI_TEXT)
        self.assertIn("python -m bandit -r src -ll -q", CI_TEXT)
        self.assertIn("python -m mypy", CI_TEXT)
        self.assertIn("python scripts/scan_secrets.py --history", CI_TEXT)
        self.assertNotIn("scripts/secret_scan.py", CI_TEXT)
        self.assertFalse(
            (PROJECT_ROOT / "scripts" / "secret_scan.py").exists(),
            "legacy weaker scanner must not coexist with the canonical hardened scanner",
        )
        self.assertIn('docker compose --env-file "$APP_ENV_FILE" config --quiet', CI_TEXT)
        self.assertNotIn("docker compose config >", CI_TEXT)

    def test_every_workflow_compose_call_uses_explicit_app_env_file(self):
        compose_lines = [
            line.strip()
            for line in (CI_TEXT + "\n" + DEPLOY_TEXT).splitlines()
            if line.strip().startswith("docker compose ")
            or re.match(r"^\s*run:\s*docker compose\s", line)
        ]
        self.assertTrue(compose_lines)
        for line in compose_lines:
            with self.subTest(line=line):
                self.assertIn('--env-file "$APP_ENV_FILE"', line)

        self.assertIn("Verify Compose env interpolation", CI_TEXT)
        self.assertIn("scripts/verify_compose_env.py", CI_TEXT)
        self.assertIn('test -f "$APP_ENV_FILE"', CI_TEXT + DEPLOY_TEXT)
        self.assertNotIn("cat \"$APP_ENV_FILE\"", CI_TEXT + DEPLOY_TEXT)

    def test_ci_direct_compose_validation_sets_synthetic_reporter_key(self):
        marker = "- name: Validate docker compose configuration"
        self.assertIn(marker, CI_TEXT)
        step = CI_TEXT.split(marker, 1)[1].split("\n      - name:", 1)[0]
        self.assertIn("env:", step)
        self.assertIn("REPORTER_SERVICE_KEY: ci_reporter_service_key", step)
        self.assertIn('docker compose --env-file "$APP_ENV_FILE" config --quiet', step)

    def test_deploy_requires_successful_ci_run_for_same_exact_sha(self):
        self.assertNotRegex(DEPLOY_TEXT, r"(?m)^\s*push:\s*$")
        self.assertIn("CI_RUN_ID", DEPLOY_TEXT)
        self.assertIn("DEPLOY_SHA", DEPLOY_TEXT)
        self.assertIn("conclusion", DEPLOY_TEXT)
        self.assertIn("head_sha", DEPLOY_TEXT)
        self.assertIn("CI", DEPLOY_TEXT)

    def test_deploy_builds_from_clean_disposable_exact_sha_checkout(self):
        for required in (
            "mktemp -d",
            "git clone --no-hardlinks",
            "git rev-parse HEAD",
            "git status --porcelain",
            "docker image inspect",
        ):
            with self.subTest(required=required):
                self.assertIn(required, DEPLOY_TEXT)
        self.assertNotIn("git pull", DEPLOY_TEXT)

    def test_deploy_enables_ollama_override_only_when_requested(self):
        self.assertIn("OLLAMA_ENABLED", DEPLOY_TEXT)
        self.assertIn("COMPOSE_FILE=", DEPLOY_TEXT)
        self.assertIn("compose.ollama.yml", DEPLOY_TEXT)
        self.assertIn('if test "$OLLAMA_ENABLED" = "true"', DEPLOY_TEXT)
        self.assertIn('OLLAMA_ENABLED="${OLLAMA_ENABLED:-false}"', DEPLOY_TEXT)

    def test_deploy_checkout_is_independent_of_operator_checkout(self):
        self.assertNotIn('git -C "$PROJECT_DIR"', DEPLOY_TEXT)
        self.assertIn('git -C "$deploy_dir" status --porcelain', DEPLOY_TEXT)
        self.assertIn('head_repository.full_name == github.repository', DEPLOY_TEXT)
        self.assertIn('bash scripts/deploy_services.sh', DEPLOY_TEXT)

    def test_deploy_checks_current_main_and_preserves_database_container(self):
        self.assertIn('git ls-remote --exit-code origin refs/heads/main', ROLLOUT)
        self.assertIn('verify_image_tag "$db_image_ref" "$db_image_id"', ROLLOUT)
        self.assertIn('test "$(running_container db)" = "$db_container_id"', ROLLOUT)
        self.assertIn('services=(reporter tracker xray-client bot)', ROLLOUT)
        self.assertIn('up -d --no-deps --no-build --pull never --wait', ROLLOUT)
        self.assertNotIn('--remove-orphans', ROLLOUT)

    def test_deploy_verifies_candidate_ids_and_restores_images_on_failure(self):
        for service in ("BOT", "TRACKER", "REPORTER", "XRAY_CLIENT"):
            self.assertIn(f"EXPECTED_{service}_IMAGE_ID", ROLLOUT)
            self.assertIn(f"ACTUAL_{service}_IMAGE_ID", ROLLOUT)
        self.assertIn('test "$check_image_id" = "$TRACKER_IMAGE"', ROLLOUT)
        self.assertIn('trap rollback_on_failure EXIT', ROLLOUT)
        self.assertIn(".State.Health.Status", ROLLOUT)

    def test_deploy_requires_verified_predeploy_backup_before_starting_services(self):
        self.assertIn("FINANCETRACKER_BACKUP_DIR", DEPLOY_TEXT)
        self.assertLess(ROLLOUT.index("./scripts/predeploy_backup.sh"), ROLLOUT.index('up -d --no-deps'))
        self.assertLess(ROLLOUT.index('migrate --check'), ROLLOUT.index('up -d --no-deps'))

    def test_codeql_and_dependabot_cover_python_and_actions(self):
        dependabot = (PROJECT_ROOT / ".github" / "dependabot.yml").read_text(encoding="utf-8")

        self.assertIn("security-events: write", CODEQL_TEXT)
        self.assertIn("languages: python", CODEQL_TEXT)
        self.assertIn("github/codeql-action/init@f52b05f4acaaa234e44466e66d29050e135ea9ef", CODEQL_TEXT)
        self.assertIn("package-ecosystem: pip", dependabot)
        self.assertIn("package-ecosystem: github-actions", dependabot)


if __name__ == "__main__":
    unittest.main()
