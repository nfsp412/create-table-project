#!/usr/bin/env python3
"""
测试 fields sheet 处理：支持同一sheet中混合使用建表语句解析和直接字段信息两种方式
"""
import sys
import unittest
from pathlib import Path
import pandas as pd

# 添加项目路径（从tests目录向上到项目根目录）
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.utils.excel_reader import _process_fields_sheet

class TestFieldsSheetProcessing(unittest.TestCase):
    def test_fields_sheet_processing_mixed_format(self):
        # 模拟Excel数据（5列混合格式）
        fields_df = pd.DataFrame(
            {
                "表名": ["table1", "table1", "table2", "table2"],
                # 新格式行字段名为空（靠建表语句解析）；旧格式行直填字段信息
                "字段名": ["", "field1", "", "field2"],
                "字段数据类型": ["", "VARCHAR(255)", "", "INT"],
                "字段注释": ["", "字段1注释", "", "字段2注释"],
                "建表语句": [
                    "CREATE TABLE table1 (`id` INT(11) COMMENT '主键', `name` VARCHAR(255) COMMENT '名称')",
                    "",
                    "CREATE TABLE table2 (`id` BIGINT COMMENT 'ID', `amount` DECIMAL(10,2) COMMENT '金额')",
                    "",
                ],
            }
        )

        result_df = _process_fields_sheet(fields_df)

        # table1: 2个解析字段 + 1个直填字段；table2 同理，总计 6
        self.assertEqual(len(result_df), 6)

        table1_fields = result_df[result_df["表名"] == "table1"]
        table2_fields = result_df[result_df["表名"] == "table2"]
        self.assertEqual(len(table1_fields), 3)
        self.assertEqual(len(table2_fields), 3)

        self.assertEqual(set(table1_fields["字段名"].tolist()), {"id", "name", "field1"})
        self.assertEqual(set(table2_fields["字段名"].tolist()), {"id", "amount", "field2"})

        # 校验直填字段信息未被改变
        field1_row = table1_fields[table1_fields["字段名"] == "field1"].iloc[0]
        self.assertEqual(field1_row["字段数据类型"], "VARCHAR(255)")
        self.assertEqual(field1_row["字段注释"], "字段1注释")

        field2_row = table2_fields[table2_fields["字段名"] == "field2"].iloc[0]
        self.assertEqual(field2_row["字段数据类型"], "INT")
        self.assertEqual(field2_row["字段注释"], "字段2注释")

    def test_same_table_multiple_create_statements_deduplicated(self):
        """同表多行建表语句（如 hive+clickhouse）应去重，避免重复字段。"""
        create_sql = "CREATE TABLE t1 (`id` BIGINT COMMENT 'ID', `name` STRING COMMENT '名称')"
        fields_df = pd.DataFrame(
            {
                "表名": ["t1", "t1"],
                "字段名": ["", ""],
                "字段数据类型": ["", ""],
                "字段注释": ["", ""],
                "建表语句": [create_sql, create_sql],
            }
        )
        result_df = _process_fields_sheet(fields_df)
        self.assertEqual(len(result_df), 2, "应去重为 2 个字段，而非 4 个")
        self.assertEqual(set(result_df["字段名"].tolist()), {"id", "name"})


if __name__ == "__main__":
    unittest.main()

