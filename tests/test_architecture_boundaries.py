import ast
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1] / "src" / "financetracker"


def imported_financetracker_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
        elif isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
    return {module for module in modules if module.startswith("financetracker.")}


class ArchitectureBoundaryTests(unittest.TestCase):
    def assert_package_does_not_import(self, package: str, forbidden: tuple[str, ...]):
        for path in (ROOT / package).glob("*.py"):
            offenders = sorted(
                module for module in imported_financetracker_modules(path)
                if any(module == prefix or module.startswith(prefix + ".") for prefix in forbidden)
            )
            self.assertEqual(offenders, [], msg=f"{path.relative_to(ROOT)}: {offenders}")

    def test_common_and_domain_are_product_layer_independent(self):
        forbidden = ("financetracker.bot", "financetracker.tracker", "financetracker.reporting", "financetracker.database")
        self.assert_package_does_not_import("common", forbidden)
        self.assert_package_does_not_import("domain", forbidden)

    def test_tracker_and_reporting_are_peer_independent(self):
        self.assert_package_does_not_import("tracker", ("financetracker.bot", "financetracker.reporting"))
        self.assert_package_does_not_import("reporting", ("financetracker.bot", "financetracker.tracker"))
