import sys
import unittest
from pathlib import Path

# 将项目根目录添加到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.utils.mysql_parser import (
    parse_create_ddl_table_name,
    parse_mysql_create_table,
    _parse_single_field,
)


class TestMysqlParser(unittest.TestCase):
    def test_parse_mysql_create_table(self):
        test_sql = """
        CREATE TABLE test_table (
            `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT '主键',
            `app_type` bigint(20) NOT NULL DEFAULT '-1' COMMENT '应用类型:1 IOS,0 Android',
            `app_url` varchar(2048) NOT NULL DEFAULT '' COMMENT '应用下载地址',
            `amount` DECIMAL(10,2) COMMENT '金额',
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='测试表';
        """

        result = parse_mysql_create_table(test_sql)
        self.assertEqual(len(result), 4)
        self.assertEqual({f["字段名"] for f in result}, {"id", "app_type", "app_url", "amount"})

        by_name = {f["字段名"]: f for f in result}
        self.assertEqual(by_name["id"]["字段数据类型"].upper(), "INT(11)")
        self.assertEqual(by_name["id"]["字段注释"], "主键")
        self.assertEqual(by_name["app_type"]["字段注释"], "应用类型:1 IOS,0 Android")

    def test_parse_single_field(self):
        cases = [
            ("`id` INT(11) COMMENT '主键'", {"字段名": "id", "字段数据类型": "INT(11)", "字段注释": "主键"}),
            (
                "`app_type` bigint(20) NOT NULL DEFAULT '-1' COMMENT '应用类型:1 IOS,0 Android'",
                {"字段名": "app_type", "字段数据类型": "bigint(20)", "字段注释": "应用类型:1 IOS,0 Android"},
            ),
            (
                "`amount` DECIMAL(10,2) COMMENT '金额'",
                {"字段名": "amount", "字段数据类型": "DECIMAL(10,2)", "字段注释": "金额"},
            ),
        ]

        for field_def, expected in cases:
            parsed = _parse_single_field(field_def)
            self.assertIsNotNone(parsed)
            self.assertEqual(parsed["字段名"], expected["字段名"])
            self.assertEqual(parsed["字段数据类型"], expected["字段数据类型"])
            self.assertEqual(parsed["字段注释"], expected["字段注释"])

        self.assertIsNone(_parse_single_field("PRIMARY KEY (`id`)"))


class TestParseCreateDdlTableName(unittest.TestCase):
    def test_mysql_backtick(self):
        self.assertEqual(parse_create_ddl_table_name("CREATE TABLE `foo` (id int)"), "foo")

    def test_mysql_simple_word(self):
        self.assertEqual(parse_create_ddl_table_name("CREATE TABLE demo_t (id int)"), "demo_t")

    def test_mysql_db_table_unquoted(self):
        self.assertEqual(parse_create_ddl_table_name("CREATE TABLE db1.tbl1 (id int)"), "tbl1")

    def test_hive_external_if_not_exists_qualified(self):
        sql = (
            "CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`ods_ad_x_day` ("
            "`id` bigint COMMENT 'pk') STORED AS ORC"
        )
        self.assertEqual(parse_create_ddl_table_name(sql), "ods_ad_x_day")

    def test_hive_external_unquoted(self):
        sql = "CREATE EXTERNAL TABLE ods_y (`a` string COMMENT 'c')"
        self.assertEqual(parse_create_ddl_table_name(sql), "ods_y")

    def test_parse_mysql_create_table_accepts_hive_header(self):
        sql = (
            "CREATE EXTERNAL TABLE IF NOT EXISTS `hive_tbl` ("
            "`id` bigint COMMENT '主键', `name` string)"
        )
        result = parse_mysql_create_table(sql)
        self.assertEqual(len(result), 2)
        by_name = {f["字段名"]: f for f in result}
        self.assertEqual(by_name["id"]["字段数据类型"].lower(), "bigint")
        self.assertEqual(by_name["name"]["字段数据类型"].lower(), "string")


if __name__ == "__main__":
    unittest.main()

