from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[1]


class ContainerSecurityContractTests(unittest.TestCase):
    def test_application_images_drop_root_before_runtime(self):
        for name in ("bot", "reporter", "tracker", "xray-client"):
            dockerfile = (ROOT / "docker" / f"Dockerfile.{name}").read_text(encoding="utf-8")
            self.assertRegex(
                dockerfile,
                re.compile(r"^FROM python:3\.12-slim@sha256:[0-9a-f]{64}$", re.MULTILINE),
                msg=dockerfile,
            )
            self.assertIn("USER financetracker", dockerfile)

    def test_application_services_use_read_only_root_and_tmpfs(self):
        compose = (ROOT / "compose.yml").read_text(encoding="utf-8")
        for service in ("xray-client", "migrate", "tracker", "bot", "reporter"):
            match = re.search(
                rf"^  {re.escape(service)}:\n(?P<body>.*?)(?=^  [a-z][a-z_-]*:\n|\Z)",
                compose,
                flags=re.MULTILINE | re.DOTALL,
            )
            self.assertIsNotNone(match)
            section = match.group("body")
            self.assertIn("read_only: true", section)
            self.assertIn("cap_drop: [ALL]", section)
            self.assertIn("no-new-privileges:true", section)
            self.assertIn("/tmp:rw,noexec,nosuid", section)

    def test_internal_proxy_and_reporter_are_not_published_to_the_host(self):
        compose = (ROOT / "compose.yml").read_text(encoding="utf-8")
        for service in ("xray-client", "reporter"):
            with self.subTest(service=service):
                section = re.search(
                    rf"^  {re.escape(service)}:\n(?P<body>.*?)(?=^  [a-z][a-z_-]*:\n|\\Z)",
                    compose,
                    flags=re.MULTILINE | re.DOTALL,
                ).group("body")
                self.assertIn("expose:", section)
                self.assertNotRegex(section, r"(?m)^    ports:")

    def test_tracker_certificates_are_installed_during_image_build(self):
        dockerfile = (ROOT / "docker" / "Dockerfile.tracker").read_text(encoding="utf-8")
        self.assertIn("COPY docker/certs/ /usr/local/share/ca-certificates/finance_tracker/", dockerfile)
        self.assertIn("RUN update-ca-certificates", dockerfile)


if __name__ == "__main__":
    unittest.main()
