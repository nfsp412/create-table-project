"""JSON 输入与 load_excel 等价性（v1.1.0）。"""
import json
import sys
import unittest
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.utils.excel_reader import process_fields_dataframe
from app.utils.input_from_json import json_items_to_dataframes, load_json_input
from app.input_models import InputData, ModifyTableInput
from app.utils.json_sql_parser import parse_input_dict, parse_json_to_items


SAMPLE_SQL = (
    "CREATE TABLE `demo_t` (`id` bigint NOT NULL) ENGINE=InnoDB COMMENT='demo';"
)


def _full_create_json() -> dict:
    return {
        "mysql_sql": SAMPLE_SQL,
        "day_or_hour": "天表",
        "product_line": "sfst",
        "dw_layer": "ods",
        "table_format": "orc",
        "target_table_format": "hive",
        "operate_type": "新建表",
        "is_sharding": "否",
    }


class TestJsonItemsToDataframes(unittest.TestCase):
    def test_roundtrip_matches_processed_fields(self):
        j = json.dumps(_full_create_json())
        items = parse_json_to_items(j)
        tables_df, fields_raw = json_items_to_dataframes(items)
        fields_proc = process_fields_dataframe(fields_raw)
        self.assertEqual(len(tables_df), 1)
        self.assertGreater(len(fields_proc), 0)
        self.assertIn("id", fields_proc["字段名"].values)


class TestLoadJsonInput(unittest.TestCase):
    def test_load_json_input_keys_match_load_excel_shape(self):
        j = json.dumps(_full_create_json())
        data = load_json_input(j)
        self.assertIn("tables", data)
        self.assertIn("fields", data)
        self.assertEqual(list(data["fields"].columns), ["表名", "字段名", "字段数据类型", "字段注释", "操作类型"])


class TestParseJsonToItemsErrors(unittest.TestCase):
    def test_empty_array_raises(self):
        with self.assertRaises(ValueError) as ctx:
            parse_json_to_items("[]")
        self.assertIn("没有可解析", str(ctx.exception))

    def test_all_items_skipped_raises(self):
        # 仅含无法解析项（无 operate_type），逐项跳过后面无数据
        j = json.dumps([{"mysql_sql": "CREATE TABLE `a` (id int);"}])
        with self.assertRaises(ValueError) as ctx:
            parse_json_to_items(j)
        self.assertIn("没有可解析", str(ctx.exception))

    def test_load_json_input_propagates(self):
        with self.assertRaises(ValueError):
            load_json_input("[]")


class TestParseInputDictRouting(unittest.TestCase):
    def test_routes_by_operate_type_new_table_requires_mysql_sql(self):
        d = {k: v for k, v in _full_create_json().items() if k != "mysql_sql"}
        d["operate_type"] = "新建表"
        self.assertIsNone(parse_input_dict(d))

    def test_routes_by_operate_type_modify_requires_new_fields(self):
        d = {
            "operate_type": "修改表",
            "target_table_format": "hive",
            "table_name": "t_alter",
        }
        self.assertIsNone(parse_input_dict(d))

    def test_modify_with_empty_new_fields_skipped(self):
        d = {
            "operate_type": "修改表",
            "target_table_format": "hive",
            "table_name": "t_alter",
            "new_fields": [],
        }
        self.assertIsNone(parse_input_dict(d))

    def test_missing_operate_type_skipped(self):
        d = {k: v for k, v in _full_create_json().items() if k != "operate_type"}
        self.assertIsNone(parse_input_dict(d))

    def test_new_table_with_both_mysql_and_new_fields_uses_new_branch(self):
        d = _full_create_json()
        d["new_fields"] = [{"field_name": "x", "field_type": "bigint"}]
        out = parse_input_dict(d)
        self.assertIsInstance(out, InputData)
        self.assertEqual(out.operate_type, "新建表")

    def test_modify_table_with_mysql_sql_still_modify_branch(self):
        d = {
            "operate_type": "修改表",
            "target_table_format": "hive",
            "table_name": "ods_ad_x_day",
            "mysql_sql": "CREATE TABLE `ignored` (id int);",
            "new_fields": [{"field_name": "c1", "field_type": "string"}],
        }
        out = parse_input_dict(d)
        self.assertIsInstance(out, ModifyTableInput)
        self.assertEqual(out.table_name, "ods_ad_x_day")


if __name__ == "__main__":
    unittest.main()
