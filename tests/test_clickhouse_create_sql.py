import sys
import unittest
from pathlib import Path

import pandas as pd

# 将项目根目录添加到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.utils.table_builder import build_create_table_sql_clickhouse, build_alter_table_sql_clickhouse


class TestClickhouseCreateTableSql(unittest.TestCase):
    def test_clickhouse_sql_contains_local_and_distributed_tables(self):
        fields_df = pd.DataFrame(
            {
                "字段名": ["id", "amount"],
                "字段数据类型": ["INT", "DECIMAL(10,2)"],
                "字段注释": ["主键", "金额"],
            }
        )

        sql = build_create_table_sql_clickhouse("ods_ad_product_test_day", fields_df)

        self.assertIn("ENGINE = ReplicatedMergeTree", sql)
        self.assertIn("ENGINE = Distributed", sql)

        self.assertIn("`dplus_hubble`.`ods_ad_product_test_day`", sql)
        self.assertIn("`dplus_hubble`.`ods_ad_product_test_day_cluster`", sql)

        # 自动追加 dt 字段
        self.assertIn("`dt` Date", sql)

        # 类型映射样例：INT -> Int64, DECIMAL -> String
        self.assertIn("`id` Int64", sql)
        self.assertIn("`amount` String", sql)

    def test_clickhouse_alter_sql_uses_add_column_syntax(self):
        fields_df = pd.DataFrame(
            {
                "字段名": ["id", "amount"],
                "字段数据类型": ["INT", "DECIMAL(10,2)"],
                "字段注释": ["主键", "金额"],
            }
        )

        sql = build_alter_table_sql_clickhouse("ods_ad_product_test_day", fields_df)

        # 本地表与分布式表均生成 alter 语句
        self.assertIn("alter table dplus_hubble.ods_ad_product_test_day on cluster logger", sql)
        self.assertIn("alter table dplus_hubble.ods_ad_product_test_day_cluster on cluster logger", sql)

        # 字段类型映射及默认值正确：INT -> Int64 DEFAULT 0，DECIMAL -> String DEFAULT ''
        self.assertIn("`id` Int64 DEFAULT 0", sql)
        self.assertIn("`amount` String DEFAULT ''", sql)

        # 本地表只应有一条 ALTER 语句，但包含多个 add column 片段
        self.assertEqual(sql.count("alter table dplus_hubble.ods_ad_product_test_day on cluster logger"), 1)
        # 分布式表也只应有一条 ALTER 语句
        self.assertEqual(sql.count("alter table dplus_hubble.ods_ad_product_test_day_cluster on cluster logger"), 1)
        # 两个字段 => 两次 add column
        self.assertGreaterEqual(sql.count("add column `id`"), 1)
        self.assertGreaterEqual(sql.count("add column `amount`"), 1)

    def test_clickhouse_order_by_uses_auto_inc_id_when_available(self):
        """存在自增ID/主键字段时，应使用该字段作为 ORDER BY 键。"""
        fields_df = pd.DataFrame(
            {
                "字段名": ["id", "amount"],
                "字段数据类型": ["INT", "DECIMAL(10,2)"],
                "字段注释": ["主键 自增", "金额"],
            }
        )

        sql = build_create_table_sql_clickhouse("ods_ad_product_test_day", fields_df)

        # 使用 id 作为 ORDER BY 字段
        self.assertIn("ORDER BY (`id`)", sql)
        # 不应再出现空的 ORDER BY ()
        self.assertNotIn("ORDER BY ()", sql)

    def test_clickhouse_order_by_empty_when_no_auto_inc_field(self):
        """不存在任何自增/主键字段时，ORDER BY 仍应为空元组。"""
        fields_df = pd.DataFrame(
            {
                "字段名": ["user_name", "age"],
                "字段数据类型": ["VARCHAR(50)", "INT"],
                "字段注释": ["用户名", "年龄"],
            }
        )

        sql = build_create_table_sql_clickhouse("ods_ad_user_test_day", fields_df)

        # 未识别到自增ID字段时保持 ORDER BY ()
        self.assertIn("ORDER BY ()", sql)
        self.assertNotIn("ORDER BY (`user_name`)", sql)
        self.assertNotIn("ORDER BY (`age`)", sql)

    def test_clickhouse_order_by_not_use_id_without_primary_key_comment(self):
        """仅字段名像 id 但注释不含主键/自增时，不应被视为自增ID字段。"""
        fields_df = pd.DataFrame(
            {
                "字段名": ["id"],
                "字段数据类型": ["INT"],
                "字段注释": ["流水号"],  # 不包含“主键/自增”等关键字
            }
        )

        sql = build_create_table_sql_clickhouse("ods_ad_flow_test_day", fields_df)

        # 由于注释未表明主键/自增，应保持 ORDER BY ()
        self.assertIn("ORDER BY ()", sql)
        self.assertNotIn("ORDER BY (`id`)", sql)


if __name__ == "__main__":
    unittest.main()

