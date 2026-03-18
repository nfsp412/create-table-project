import sys
import unittest
from pathlib import Path

import pandas as pd

# 将项目根目录添加到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.utils.table_builder import build_create_table_sql


def _fields_df_minimal():
    return pd.DataFrame(
        {
            "字段名": ["id"],
            "字段数据类型": ["INT(11)"],
            "字段注释": ["主键"],
        }
    )


class TestHiveCreateTableSql(unittest.TestCase):
    def test_partition_clause_day(self):
        sql = build_create_table_sql(
            hive_table_name="ods_ad_product_test_day",
            table_comment="测试表",
            fields_df=_fields_df_minimal(),
            load_type="天表",
            storage_format="rcfile",
            dw_layer="ods",
        )
        self.assertIn("PARTITIONED BY (`dt` string)", sql)
        self.assertNotIn("PARTITIONED BY (`dt` string, `hour` string)", sql)

    def test_partition_clause_hour(self):
        sql = build_create_table_sql(
            hive_table_name="ods_ad_product_test_hour",
            table_comment="测试表",
            fields_df=_fields_df_minimal(),
            load_type="小时表",
            storage_format="rcfile",
            dw_layer="ods",
        )
        self.assertIn("PARTITIONED BY (`dt` string, `hour` string)", sql)

    def test_storage_format_rcfile(self):
        sql = build_create_table_sql(
            hive_table_name="ods_ad_product_test_day",
            table_comment="",
            fields_df=_fields_df_minimal(),
            load_type="天表",
            storage_format="rcfile",
            dw_layer="ods",
        )
        self.assertIn("STORED AS RCFILE", sql)
        self.assertNotIn("TBLPROPERTIES('orc.compress'='SNAPPY')", sql)

    def test_storage_format_text(self):
        sql = build_create_table_sql(
            hive_table_name="ods_ad_product_test_day",
            table_comment="",
            fields_df=_fields_df_minimal(),
            load_type="天表",
            storage_format="text",
            dw_layer="ods",
        )
        self.assertIn("STORED AS TEXTFILE", sql)
        self.assertNotIn("TBLPROPERTIES('orc.compress'='SNAPPY')", sql)

    def test_storage_format_orc(self):
        sql = build_create_table_sql(
            hive_table_name="ods_ad_product_test_day",
            table_comment="",
            fields_df=_fields_df_minimal(),
            load_type="天表",
            storage_format="orc",
            dw_layer="ods",
        )
        self.assertIn("STORED AS ORC", sql)
        self.assertIn("TBLPROPERTIES('orc.compress'='SNAPPY')", sql)

    def test_location_uses_dw_layer(self):
        sql_ods = build_create_table_sql(
            hive_table_name="ods_ad_product_test_day",
            table_comment="",
            fields_df=_fields_df_minimal(),
            load_type="天表",
            storage_format="rcfile",
            dw_layer="ods",
        )
        self.assertIn("LOCATION 'viewfs://c9/dw/ods/ods_ad_product_test_day'", sql_ods)

        sql_sds = build_create_table_sql(
            hive_table_name="ods_ad_product_test_day",
            table_comment="",
            fields_df=_fields_df_minimal(),
            load_type="天表",
            storage_format="rcfile",
            dw_layer="sds",
        )
        self.assertIn("LOCATION 'viewfs://c9/dw/sds/ods_ad_product_test_day'", sql_sds)

    def test_table_comment_suffix_day_hour(self):
        sql_day = build_create_table_sql(
            hive_table_name="ods_ad_product_test_day",
            table_comment="测试表",
            fields_df=_fields_df_minimal(),
            load_type="天表",
            storage_format="rcfile",
            dw_layer="ods",
        )
        self.assertIn("COMMENT '测试表天表'", sql_day)

        sql_hour = build_create_table_sql(
            hive_table_name="ods_ad_product_test_hour",
            table_comment="测试表",
            fields_df=_fields_df_minimal(),
            load_type="小时表",
            storage_format="rcfile",
            dw_layer="ods",
        )
        self.assertIn("COMMENT '测试表小时表'", sql_hour)


if __name__ == "__main__":
    unittest.main()

