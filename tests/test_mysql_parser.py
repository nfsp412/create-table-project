import sys
import unittest
from pathlib import Path

# 将项目根目录添加到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.utils.mysql_parser import parse_mysql_create_table, _parse_single_field


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


if __name__ == "__main__":
    unittest.main()

