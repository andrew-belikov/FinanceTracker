import unittest
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from common.text_utils import has_mojibake, try_repair_cp866_utf8


class TextUtilsTests(unittest.TestCase):
    def test_has_mojibake_detects_broken_cyrillic(self):
        self.assertTrue(has_mojibake("╨Я╨╛╨┐╨╛╨╗╨╜╨╡╨╜╨╕╨╡"))
        self.assertFalse(has_mojibake("Пополнение счета"))

    def test_try_repair_cp866_utf8_repairs_known_sequence(self):
        broken = "╨Я╨╛╨┐╨╛╨╗╨╜╨╡╨╜╨╕╨╡ ╤Б╤З╨╡╤В╨░"
        self.assertEqual(try_repair_cp866_utf8(broken), "Пополнение счета")

    def test_try_repair_cp866_utf8_leaves_clean_text_unchanged(self):
        clean = "ВИМ - Ликвидность"
        self.assertEqual(try_repair_cp866_utf8(clean), clean)


if __name__ == "__main__":
    unittest.main()
