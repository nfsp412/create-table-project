"""表构建工具测试"""
import sys
import unittest
from pathlib import Path

# 将项目根目录添加到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.utils.table_builder import build_table_name, normalize_load_type


class TestTableBuilder(unittest.TestCase):
    """表构建测试类"""

    def test_normalize_load_type(self):
        """测试入仓方式标准化"""
        self.assertEqual(normalize_load_type("天表"), "day")
        self.assertEqual(normalize_load_type("小时表"), "hour")
        self.assertEqual(normalize_load_type("day"), "day")
        self.assertEqual(normalize_load_type("hour"), "hour")

    def test_build_table_name(self):
        """测试表名构建"""
        result = build_table_name("test_table", "product", "天表")
        self.assertEqual(result, "ods_ad_product_test_table_day")
        
        result = build_table_name("test_table", "product", "小时表")
        self.assertEqual(result, "ods_ad_product_test_table_hour")


if __name__ == "__main__":
    unittest.main()
