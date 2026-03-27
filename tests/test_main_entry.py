"""main() CLI：JSON 路径、解析失败退出码。"""
import shutil
import sys
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import MagicMock, patch

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


class TestMainJsonExit(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_main_json_string_no_valid_items_exits_1(self):
        import app.main as main_mod

        mock_dt = MagicMock()
        mock_dt.now.return_value.strftime.return_value = "20260318"

        with patch.object(main_mod, "parse_args", return_value=Namespace(
            input_excel=None,
            json_file=None,
            json_string="[]",
            output_dir=self.tmp / "out",
            debug=False,
        )), patch.object(main_mod, "setup_logging"), patch.object(
            main_mod, "datetime", mock_dt
        ):
            with self.assertRaises(SystemExit) as ctx:
                main_mod.main()
        self.assertEqual(ctx.exception.code, 1)

    def test_main_json_file_not_found_exits_1(self):
        import app.main as main_mod

        missing = self.tmp / "missing.json"
        mock_dt = MagicMock()
        mock_dt.now.return_value.strftime.return_value = "20260318"

        with patch.object(main_mod, "parse_args", return_value=Namespace(
            input_excel=None,
            json_file=missing,
            json_string=None,
            output_dir=self.tmp / "out2",
            debug=False,
        )), patch.object(main_mod, "setup_logging"), patch.object(
            main_mod, "datetime", mock_dt
        ):
            with self.assertRaises(SystemExit) as ctx:
                main_mod.main()
        self.assertEqual(ctx.exception.code, 1)


if __name__ == "__main__":
    unittest.main()
