import sys
import unittest
from pathlib import Path
import pandas as pd

# 将项目根目录添加到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.utils.excel_reader import _process_fields_sheet
from app.utils.table_builder import build_create_table_sql


class TestEmptyFieldComment(unittest.TestCase):
    def test_process_fields_sheet_allows_empty_comment(self):
        fields_df = pd.DataFrame(
            {
                "表名": ["table1"],
                "字段名": ["id"],
                "字段数据类型": ["INT(11)"],
                "字段注释": [""],
                "建表语句": [""],
            }
        )

        result_df = _process_fields_sheet(fields_df)

        self.assertEqual(len(result_df), 1)
        self.assertEqual(result_df.iloc[0]["表名"], "table1")
        self.assertEqual(result_df.iloc[0]["字段名"], "id")
        self.assertEqual(result_df.iloc[0]["字段数据类型"], "INT(11)")
        self.assertEqual(result_df.iloc[0]["字段注释"], "")

    def test_build_create_table_sql_omits_comment_when_empty(self):
        fields_df = pd.DataFrame(
            {
                "表名": ["table1"],
                "字段名": ["id"],
                "字段数据类型": ["INT(11)"],
                "字段注释": [""],
                "建表语句": [""],
            }
        )
        processed_df = _process_fields_sheet(fields_df)

        sql = build_create_table_sql(
            hive_table_name="ods_ad_product_table1_day",
            table_comment="",
            fields_df=processed_df,
            load_type="天表",
            storage_format="rcfile",
            dw_layer="ods",
        )

        self.assertIn("`id` BIGINT", sql)
        self.assertNotIn("`id` BIGINT COMMENT", sql)


if __name__ == "__main__":
    unittest.main()

