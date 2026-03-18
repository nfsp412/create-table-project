import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

# 将项目根目录添加到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.utils.excel_reader import load_excel


class TestLoadExcel(unittest.TestCase):
    def test_load_excel_raises_when_file_missing(self):
        with patch("app.utils.excel_reader.os.path.exists", return_value=False):
            with self.assertRaises(FileNotFoundError):
                load_excel()

    def test_load_excel_raises_when_fields_missing_required_columns(self):
        tables_df = pd.DataFrame({"表名": ["t1"], "产品线": ["p"], "入仓方式": ["天表"]})
        # 缺少 “字段注释” 列
        fields_df = pd.DataFrame(
            {
                "表名": ["t1"],
                "字段名": ["id"],
                "字段数据类型": ["INT"],
                "建表语句": [""],
            }
        )

        fake_xls = MagicMock()

        def _fake_read_excel(_xls, sheet_name):
            if sheet_name == "tables":
                return tables_df
            if sheet_name == "fields":
                return fields_df
            raise ValueError(f"unexpected sheet: {sheet_name}")

        with patch("app.utils.excel_reader.os.path.exists", return_value=True), patch(
            "app.utils.excel_reader.pd.ExcelFile", return_value=fake_xls
        ), patch("app.utils.excel_reader.pd.read_excel", side_effect=_fake_read_excel):
            with self.assertRaises(ValueError) as ctx:
                load_excel()
            self.assertIn("fields sheet 缺少必需的列", str(ctx.exception))
            self.assertIn("字段注释", str(ctx.exception))

    def test_load_excel_happy_path_returns_processed_fields(self):
        tables_df = pd.DataFrame({"表名": ["t1"], "产品线": ["p"], "入仓方式": ["天表"]})
        fields_df = pd.DataFrame(
            {
                "表名": ["t1"],
                "字段名": ["id"],
                "字段数据类型": ["INT(11)"],
                "字段注释": [""],
                "建表语句": [""],
            }
        )

        fake_xls = MagicMock()

        def _fake_read_excel(_xls, sheet_name):
            if sheet_name == "tables":
                return tables_df
            if sheet_name == "fields":
                return fields_df
            raise ValueError(f"unexpected sheet: {sheet_name}")

        with patch("app.utils.excel_reader.os.path.exists", return_value=True), patch(
            "app.utils.excel_reader.pd.ExcelFile", return_value=fake_xls
        ), patch("app.utils.excel_reader.pd.read_excel", side_effect=_fake_read_excel):
            result = load_excel()

        self.assertIn("tables", result)
        self.assertIn("fields", result)

        processed_fields = result["fields"]
        self.assertEqual(len(processed_fields), 1)
        self.assertEqual(processed_fields.iloc[0]["表名"], "t1")
        self.assertEqual(processed_fields.iloc[0]["字段名"], "id")
        self.assertEqual(processed_fields.iloc[0]["字段数据类型"], "INT(11)")
        self.assertEqual(processed_fields.iloc[0]["字段注释"], "")


if __name__ == "__main__":
    unittest.main()

