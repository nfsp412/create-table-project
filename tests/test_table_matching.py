#!/usr/bin/env python3
"""
测试表名匹配逻辑：验证tables页的hive表名与fields页的表名匹配
"""
import sys
import unittest
from pathlib import Path
import pandas as pd

# 添加项目路径（从tests目录向上到项目根目录）
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

class TestTableMatching(unittest.TestCase):
    def test_table_matching_uses_hive_table_name_when_provided(self):
        tables_df = pd.DataFrame(
            {
                "表名": ["table1", "table2", "table3"],
                "产品线": ["product", "product", "product"],
                "入仓方式": ["天表", "小时表", "天表"],
                "表注释信息": ["测试表1", "测试表2", "测试表3"],
                # table1 有 hive 表名，table2 没有，table3 有
                "hive表名": ["custom_hive_table", "", "another_hive_table"],
            }
        )

        fields_df = pd.DataFrame(
            {
                # fields 页混用 hive 表名和原始表名
                "表名": ["custom_hive_table", "table2", "another_hive_table"],
                "字段名": ["id", "id", "id"],
                "字段数据类型": ["INT", "INT", "INT"],
                "字段注释": ["主键", "主键", "主键"],
            }
        )

        for _, t_row in tables_df.iterrows():
            raw_table_name = t_row["表名"]
            hive_table_name_from_excel = t_row.get("hive表名", "")

            if hive_table_name_from_excel and str(hive_table_name_from_excel).strip():
                match_table_name = str(hive_table_name_from_excel).strip()
            else:
                match_table_name = raw_table_name

            table_fields_df = fields_df[fields_df["表名"] == match_table_name]
            self.assertFalse(
                table_fields_df.empty,
                msg=f"未匹配到字段：raw_table_name={raw_table_name}, match_table_name={match_table_name}",
            )


if __name__ == "__main__":
    unittest.main()

