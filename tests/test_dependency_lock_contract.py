from pathlib import Path
import re
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOCKS = sorted((PROJECT_ROOT / "requirements").glob("*.txt"))


class DependencyLockContractTests(unittest.TestCase):
    def test_every_runtime_lock_is_exact_and_hash_verified(self):
        self.assertEqual([path.name for path in LOCKS], ["bot.txt", "reporter.txt", "tracker.txt"])
        for path in LOCKS:
            text = path.read_text(encoding="utf-8")
            starts = list(
                re.finditer(
                    r"(?m)^(?P<requirement>[A-Za-z0-9_.-]+(?:\[[^]]+\])?==[^\s\\]+)",
                    text,
                )
            )
            self.assertTrue(starts, path.name)
            for index, match in enumerate(starts):
                block_end = starts[index + 1].start() if index + 1 < len(starts) else len(text)
                block = text[match.start():block_end]
                with self.subTest(path=path.name, requirement=match.group("requirement")):
                    self.assertRegex(match.group("requirement"), r"==[^\s\\]+$")
                    self.assertRegex(block, r"--hash=sha256:[0-9a-f]{64}")


if __name__ == "__main__":
    unittest.main()
