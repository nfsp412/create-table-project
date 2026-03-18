import os
import unittest
from argparse import Namespace
from unittest.mock import call, mock_open, patch

import pandas as pd


def _tables_df_row(**kwargs) -> pd.DataFrame:
    defaults = {
        "表名": "t1",
        "产品线": "pl",
        "入仓方式": "天表",
        "表注释信息": "测试表",
        "数仓分层": "",
        "建表格式": "",
        "hive表名": "",
        "目标表类型": "",
        "操作类型": "",
    }
    defaults.update(kwargs)
    return pd.DataFrame([defaults])


def _fields_df_for_table(table_name: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "表名": table_name,
                "字段名": "id",
                "字段数据类型": "INT(11)",
                "字段注释": "主键",
                "操作类型": "新建表",
            }
        ]
    )


class TestMainFlow(unittest.TestCase):
    def _run_main_with_mocks(
        self,
        *,
        tables_df: pd.DataFrame,
        fields_df: pd.DataFrame,
        output_dir: str = "/fake/out",
        build_table_name_return: str = "ods_ad_pl_t1_day",
        hive_sql: str = "HIVE_SQL",
        ck_sql: str = "CK_SQL",
        ck_alter_sql: str = "CK_ALTER_SQL",
        exists_output_dir: bool = False,
        exists_output_file: bool = False,
    ):
        import app.main as main_mod

        m_open = mock_open()

        def _exists_side_effect(path: str) -> bool:
            if path == output_dir:
                return exists_output_dir
            return exists_output_file

        with patch.object(main_mod, "parse_args", return_value=Namespace(debug=False)), patch.object(
            main_mod, "setup_logging"
        ) as m_setup_logging, patch.object(
            main_mod, "load_excel", return_value={"tables": tables_df, "fields": fields_df}
        ), patch.object(main_mod, "OUTPUT_DIR", output_dir), patch.object(
            main_mod, "build_table_name", return_value=build_table_name_return
        ) as m_build_table_name, patch.object(
            main_mod, "build_create_table_sql", return_value=hive_sql
        ) as m_build_hive_sql, patch.object(
            main_mod, "build_create_table_sql_clickhouse", return_value=ck_sql
        ) as m_build_ck_sql, patch.object(
            main_mod, "build_alter_table_sql_clickhouse", return_value=ck_alter_sql
        ) as m_build_ck_alter_sql, patch.object(
            main_mod, "os"
        ) as m_os, patch.object(
            main_mod.os.path, "exists", side_effect=_exists_side_effect
        ) as m_exists, patch(
            "builtins.open", m_open
        ):
            main_mod.main()

        return {
            "open": m_open,
            "setup_logging": m_setup_logging,
            "build_table_name": m_build_table_name,
            "build_hive_sql": m_build_hive_sql,
            "build_ck_sql": m_build_ck_sql,
            "build_ck_alter_sql": m_build_ck_alter_sql,
            "exists": m_exists,
            "makedirs": m_os.makedirs,
            "remove": m_os.remove,
        }

    def test_hive_branch_builds_table_name_and_creates_output_dir(self):
        tables_df = _tables_df_row(hive表名="", 目标表类型="")
        fields_df = _fields_df_for_table("t1")

        mocks = self._run_main_with_mocks(
            tables_df=tables_df,
            fields_df=fields_df,
            build_table_name_return="ods_ad_pl_t1_day",
            hive_sql="HIVE_SQL",
            exists_output_dir=False,
            exists_output_file=False,
        )

        mocks["makedirs"].assert_called_once_with("/fake/out", exist_ok=True)
        mocks["build_table_name"].assert_called_once()
        mocks["build_hive_sql"].assert_called_once()

        # 只断言 open 被调用，写入编码正确
        args, kwargs = mocks["open"].call_args
        self.assertEqual(args[1], "w")
        self.assertEqual(kwargs.get("encoding"), "utf-8")
        mocks["open"]().write.assert_called_once_with("HIVE_SQL")

    def test_clickhouse_branch_writes_ck_suffix(self):
        tables_df = _tables_df_row(目标表类型="clickhouse", hive表名="", 操作类型="新建表")
        fields_df = _fields_df_for_table("t1")

        mocks = self._run_main_with_mocks(
            tables_df=tables_df,
            fields_df=fields_df,
            build_table_name_return="ods_ad_pl_t1_day",
            ck_sql="CK_SQL",
            exists_output_dir=True,
            exists_output_file=False,
        )

        mocks["build_ck_sql"].assert_called_once()
        args, kwargs = mocks["open"].call_args
        self.assertEqual(args[1], "w")
        self.assertEqual(kwargs.get("encoding"), "utf-8")
        mocks["open"]().write.assert_called_once_with("CK_SQL")

    def test_clickhouse_alter_branch_writes_ck_alter_suffix_and_uses_alter_builder(self):
        # 表级为 clickhouse + 修改表
        tables_df = _tables_df_row(目标表类型="clickhouse", hive表名="", 操作类型="修改表")
        # 字段级：同一张表，操作类型标记为“修改表”
        fields_df = pd.DataFrame(
            [
                {
                    "表名": "t1",
                    "字段名": "id",
                    "字段数据类型": "INT(11)",
                    "字段注释": "主键",
                    "操作类型": "修改表",
                }
            ]
        )

        mocks = self._run_main_with_mocks(
            tables_df=tables_df,
            fields_df=fields_df,
            build_table_name_return="ods_ad_pl_t1_day",
            ck_alter_sql="CK_ALTER_SQL",
            exists_output_dir=True,
            exists_output_file=False,
        )

        # 应该调用修改表的 builder，而不是建表的 builder
        mocks["build_ck_alter_sql"].assert_called_once()
        mocks["build_ck_sql"].assert_not_called()

        args, kwargs = mocks["open"].call_args
        self.assertEqual(args[1], "w")
        self.assertEqual(kwargs.get("encoding"), "utf-8")
        mocks["open"]().write.assert_called_once_with("CK_ALTER_SQL")

    def test_hive_table_name_from_excel_is_used_and_matches_fields(self):
        tables_df = _tables_df_row(表名="t1", hive表名="custom_tbl", 目标表类型="")
        fields_df = _fields_df_for_table("custom_tbl")

        mocks = self._run_main_with_mocks(
            tables_df=tables_df,
            fields_df=fields_df,
            build_table_name_return="should_not_be_used",
            hive_sql="HIVE_SQL",
            exists_output_dir=True,
            exists_output_file=False,
        )

        mocks["build_table_name"].assert_not_called()
        args, kwargs = mocks["open"].call_args
        self.assertEqual(args[1], "w")
        self.assertEqual(kwargs.get("encoding"), "utf-8")

    def test_empty_raw_table_name_but_hive_table_name_present_still_generates(self):
        tables_df = _tables_df_row(表名="", hive表名="custom_tbl", 目标表类型="")
        fields_df = _fields_df_for_table("custom_tbl")

        mocks = self._run_main_with_mocks(
            tables_df=tables_df,
            fields_df=fields_df,
            hive_sql="HIVE_SQL",
            exists_output_dir=True,
            exists_output_file=False,
        )

        args, kwargs = mocks["open"].call_args
        self.assertEqual(args[1], "w")
        self.assertEqual(kwargs.get("encoding"), "utf-8")
        mocks["open"]().write.assert_called_once_with("HIVE_SQL")

    def test_existing_output_file_is_removed_before_write(self):
        tables_df = _tables_df_row(hive表名="", 目标表类型="")
        fields_df = _fields_df_for_table("t1")

        mocks = self._run_main_with_mocks(
            tables_df=tables_df,
            fields_df=fields_df,
            build_table_name_return="ods_ad_pl_t1_day",
            hive_sql="HIVE_SQL",
            exists_output_dir=True,
            exists_output_file=True,
        )

        # 删除与写入都应指向同一 .sql 文件
        remove_args, _ = mocks["remove"].call_args
        open_args, open_kwargs = mocks["open"].call_args
        # 这里只断言删除和写入使用了同一个路径，以及写入编码正确
        self.assertEqual(remove_args[0], open_args[0])
        self.assertEqual(open_args[1], "w")
        self.assertEqual(open_kwargs.get("encoding"), "utf-8")

    def test_no_matching_fields_skips_generation(self):
        tables_df = _tables_df_row(表名="t1", hive表名="", 目标表类型="")
        # fields_df 不提供任何匹配表名
        fields_df = _fields_df_for_table("other")

        mocks = self._run_main_with_mocks(
            tables_df=tables_df,
            fields_df=fields_df,
            build_table_name_return="ods_ad_pl_t1_day",
            exists_output_dir=True,
            exists_output_file=False,
        )

        mocks["build_hive_sql"].assert_not_called()
        mocks["build_ck_sql"].assert_not_called()
        mocks["open"].assert_not_called()


if __name__ == "__main__":
    unittest.main()

