"""Hive 加字段 SQL 生成测试"""
import sys
import unittest
from pathlib import Path

import pandas as pd

# 将项目根目录添加到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.utils.table_builder import build_alter_table_sql_hive


class TestHiveAlterTableSql(unittest.TestCase):
    def test_hive_alter_sql_uses_add_columns_syntax(self):
        """单字段：生成 ADD COLUMNS，包含正确类型和 COMMENT"""
        fields_df = pd.DataFrame(
            {
                "字段名": ["new_col"],
                "字段数据类型": ["VARCHAR(100)"],
                "字段注释": ["新字段"],
            }
        )

        sql = build_alter_table_sql_hive("ods_ad_product_test_day", fields_df)

        self.assertIn("ALTER TABLE `default`.`ods_ad_product_test_day` ADD COLUMNS", sql)
        self.assertIn("`new_col` STRING", sql)
        self.assertIn("COMMENT '新字段'", sql)

    def test_hive_alter_sql_multiple_fields(self):
        """多字段：一条 SQL 中包含多个 add columns 片段"""
        fields_df = pd.DataFrame(
            {
                "字段名": ["id", "amount"],
                "字段数据类型": ["INT", "DECIMAL(10,2)"],
                "字段注释": ["主键", "金额"],
            }
        )

        sql = build_alter_table_sql_hive("ods_ad_product_test_day", fields_df)

        self.assertIn("ALTER TABLE `default`.`ods_ad_product_test_day` ADD COLUMNS", sql)
        self.assertIn("`id` BIGINT", sql)
        self.assertIn("`amount` STRING", sql)
        self.assertIn("COMMENT '主键'", sql)
        self.assertIn("COMMENT '金额'", sql)
        self.assertEqual(sql.upper().count("ADD COLUMNS"), 1)

    def test_hive_alter_sql_empty_fields_returns_empty(self):
        """空字段列表：返回空字符串"""
        fields_df = pd.DataFrame(
            columns=["字段名", "字段数据类型", "字段注释"],
        )

        sql = build_alter_table_sql_hive("ods_ad_product_test_day", fields_df)

        self.assertEqual(sql, "")

    def test_hive_alter_sql_type_mapping(self):
        """类型映射：INT -> BIGINT、VARCHAR -> STRING 与 build_create_table_sql 一致"""
        fields_df = pd.DataFrame(
            {
                "字段名": ["id", "name", "price"],
                "字段数据类型": ["INT(11)", "VARCHAR(50)", "DECIMAL(10,2)"],
                "字段注释": ["主键", "名称", "价格"],
            }
        )

        sql = build_alter_table_sql_hive("ods_ad_product_test_day", fields_df)

        self.assertIn("`id` BIGINT", sql)
        self.assertIn("`name` STRING", sql)
        self.assertIn("`price` STRING", sql)


if __name__ == "__main__":
    unittest.main()
