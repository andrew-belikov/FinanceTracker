import re
from pathlib import Path
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CI_TEXT = (PROJECT_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
DEPLOY_TEXT = (PROJECT_ROOT / ".github" / "workflows" / "deploy.yml").read_text(encoding="utf-8")


class WorkflowContractTests(unittest.TestCase):
    def test_all_actions_are_pinned_to_immutable_commit_sha(self):
        uses = re.findall(r"^\s*uses:\s*([^\s#]+)", CI_TEXT + "\n" + DEPLOY_TEXT, re.MULTILINE)
        self.assertTrue(uses)
        for action in uses:
            with self.subTest(action=action):
                self.assertRegex(action, r"^[^@]+@[0-9a-f]{40}$")

    def test_ci_installs_hash_locked_dependencies_and_runs_explicit_gates(self):
        self.assertIn("--require-hashes", CI_TEXT)
        self.assertIn("Run workflow contract checks", CI_TEXT)
        self.assertIn("Run secret scan", CI_TEXT)
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

    def test_dirty_canonical_checkout_is_rejected_before_disposable_checkout(self):
        clean_check = 'git -C "$PROJECT_DIR" status --porcelain --untracked-files=all'
        self.assertIn(clean_check, DEPLOY_TEXT)
        self.assertLess(DEPLOY_TEXT.index(clean_check), DEPLOY_TEXT.index("mktemp -d"))

    def test_deploy_compares_running_container_images_with_captured_build_ids(self):
        for service in ("bot", "tracker", "reporter", "xray-client", "migrate"):
            with self.subTest(service=service):
                self.assertIn(f'verify_container_image "{service}"', DEPLOY_TEXT)
        self.assertIn("EXPECTED_BOT_IMAGE_ID", DEPLOY_TEXT)
        self.assertIn("EXPECTED_TRACKER_IMAGE_ID", DEPLOY_TEXT)
        self.assertIn("EXPECTED_REPORTER_IMAGE_ID", DEPLOY_TEXT)
        self.assertIn("EXPECTED_XRAY_CLIENT_IMAGE_ID", DEPLOY_TEXT)
        self.assertIn("docker inspect --format '{{.Image}}'", DEPLOY_TEXT)
        self.assertIn('export BOT_IMAGE="$EXPECTED_BOT_IMAGE_ID"', DEPLOY_TEXT)
        self.assertIn('export TRACKER_IMAGE="$EXPECTED_TRACKER_IMAGE_ID"', DEPLOY_TEXT)
        self.assertLess(
            DEPLOY_TEXT.index("docker compose --env-file \"$APP_ENV_FILE\" up"),
            DEPLOY_TEXT.index('verify_container_image "bot"'),
        )


if __name__ == "__main__":
    unittest.main()
