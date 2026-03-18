"""类型转换工具测试"""
import sys
import unittest
from pathlib import Path

# 将项目根目录添加到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.utils.type_converter import mysql_type_to_hive


class TestTypeConverter(unittest.TestCase):
    """类型转换测试类"""

    def test_bigint_types(self):
        """测试整数类型转换"""
        self.assertEqual(mysql_type_to_hive("BIGINT"), "BIGINT")
        self.assertEqual(mysql_type_to_hive("BIGINT(20)"), "BIGINT")
        self.assertEqual(mysql_type_to_hive("INT"), "BIGINT")
        self.assertEqual(mysql_type_to_hive("INT(11)"), "BIGINT")
        self.assertEqual(mysql_type_to_hive("TINYINT"), "BIGINT")
        self.assertEqual(mysql_type_to_hive("INTEGER"), "BIGINT")

    def test_float_types(self):
        """测试浮点类型转换（统一转换为STRING）"""
        self.assertEqual(mysql_type_to_hive("FLOAT"), "STRING")
        self.assertEqual(mysql_type_to_hive("FLOAT(10,2)"), "STRING")
        self.assertEqual(mysql_type_to_hive("DOUBLE"), "STRING")
        self.assertEqual(mysql_type_to_hive("DOUBLE(16,8)"), "STRING")

    def test_decimal_types(self):
        """测试DECIMAL类型转换（精确数值类型统一转换为STRING）"""
        self.assertEqual(mysql_type_to_hive("DECIMAL"), "STRING")
        self.assertEqual(mysql_type_to_hive("DECIMAL(22,4)"), "STRING")
        self.assertEqual(mysql_type_to_hive("DECIMAL(10,2)"), "STRING")
        self.assertEqual(mysql_type_to_hive("NUMERIC"), "STRING")
        self.assertEqual(mysql_type_to_hive("NUMERIC(10,2)"), "STRING")

    def test_string_types(self):
        """测试字符串类型转换"""
        self.assertEqual(mysql_type_to_hive("VARCHAR(255)"), "STRING")
        self.assertEqual(mysql_type_to_hive("TEXT"), "STRING")
        self.assertEqual(mysql_type_to_hive("CHAR"), "STRING")
        self.assertEqual(mysql_type_to_hive(""), "STRING")
        self.assertEqual(mysql_type_to_hive(None), "STRING")


if __name__ == "__main__":
    unittest.main()
