from __future__ import annotations

import hashlib
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
APPLIED_MIGRATION_HASHES = {
    "migrations/20260221_operations_from_deposits.sql": (
        "fb54cfb8d58c71c406eec17f126de772b42940c532e347ac244c386eb2481b96"
    ),
    "migrations/20260225_operations_add_instrument_columns.sql": (
        "3b2c24d8dbe5cf934c19e630723225d3d78edf703ea02f082caae0f29da747a4"
    ),
    "migrations/20260226_income_events.sql": (
        "a577088fdc8c56955240370637f7c8e559efdf1ced7f6072be7e45b3eb93e0eb"
    ),
    "migrations/20260304_operations_operation_item_fields.sql": (
        "b517e02b2a1f01007aaafe77f820a8920c5611ed627dc841c8077424d014c600"
    ),
}


class AppliedMigrationByteIdentityTests(unittest.TestCase):
    def test_applied_migrations_keep_production_ledger_bytes(self) -> None:
        for relative, expected_hash in APPLIED_MIGRATION_HASHES.items():
            with self.subTest(path=relative):
                data = (ROOT / relative).read_bytes()
                self.assertEqual(hashlib.sha256(data).hexdigest(), expected_hash)
                self.assertNotIn(b"\n", data.replace(b"\r\n", b""))

    def test_applied_migrations_are_checked_out_with_crlf(self) -> None:
        for relative in APPLIED_MIGRATION_HASHES:
            with self.subTest(path=relative):
                output = subprocess.check_output(
                    ["git", "check-attr", "eol", "--", relative],
                    cwd=ROOT,
                    text=True,
                ).strip()
                self.assertEqual(output, f"{relative}: eol: crlf")


if __name__ == "__main__":
    unittest.main()
