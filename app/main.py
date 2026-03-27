#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
主程序入口：支持 --input-excel 或 --json-file / --json-string，生成 SQL 并写 RPA。
"""
import argparse
import logging
import os
import sys
from pathlib import Path

import pandas as pd

_current_file = Path(__file__).resolve()
project_root = _current_file.parent.parent

if not (project_root / "setup.py").exists() and not (project_root / "requirements.txt").exists():
    cwd = Path.cwd()
    if (cwd / "setup.py").exists() or (cwd / "requirements.txt").exists():
        project_root = cwd

project_root_str = str(project_root.resolve())
if project_root_str not in sys.path:
    sys.path.insert(0, project_root_str)

from datetime import datetime

from app.config.settings import EXCEL_FILENAME, OUTPUT_BASE_DIR
from app.utils.excel_reader import load_excel
from app.utils.input_from_json import load_json_input
from app.utils.logger import setup_logging
from app.utils.rpa_sheet import build_rpa_row, write_rpa_sheet
from app.utils.table_builder import (
    build_create_table_sql,
    build_table_name,
    build_create_table_sql_clickhouse,
    build_alter_table_sql_clickhouse,
    build_alter_table_sql_hive,
)

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="根据 Excel 或 JSON 配置生成 Hive/ClickHouse 建表或 ALTER SQL，并写入 RPA sheet"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--input-excel",
        type=Path,
        metavar="PATH",
        help="输入 Excel 路径（含 tables、fields sheet）",
    )
    mode.add_argument(
        "--json-file",
        type=Path,
        metavar="PATH",
        help="输入 JSON 文件路径（与 add-sql-to-excel 格式一致）",
    )
    mode.add_argument(
        "--json-string",
        metavar="JSON",
        help="直接传入 JSON 字符串",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help=f"输出目录（默认：{OUTPUT_BASE_DIR}/YYYYMMDD）",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="开启 DEBUG 日志等级",
    )
    return parser.parse_args()


def run_generation(
    output_dir: Path,
    excel_path_for_rpa: Path,
    tables_df: pd.DataFrame,
    fields_df: pd.DataFrame,
) -> None:
    """根据已处理的 tables_df、fields_df 生成 .sql 并写 RPA。"""
    rpa_rows: list[dict[str, str]] = []

    for _, t_row in tables_df.iterrows():
        raw_table_name = str(t_row["表名"]).strip() if not pd.isna(t_row["表名"]) else ""

        product_line = t_row["产品线"]
        load_type = t_row["入仓方式"]
        table_comment = t_row.get("表注释信息", "")
        dw_layer_raw = t_row.get("数仓分层", "")
        dw_layer = "ods"
        if not pd.isna(dw_layer_raw) and str(dw_layer_raw).strip() and str(dw_layer_raw).strip().lower() != "nan":
            dw_layer = str(dw_layer_raw).strip().lower()

        storage_format_raw = t_row.get("建表格式", "")
        storage_format = "rcfile"
        if not pd.isna(storage_format_raw):
            sf = str(storage_format_raw).strip().lower()
            if sf and sf != "nan":
                if sf in {"text", "orc", "rcfile"}:
                    storage_format = sf
                else:
                    logger.warning("未知建表格式 '%s'，表 %s 将使用默认 RCFILE 格式", storage_format_raw, raw_table_name)

        target_table_type_raw = t_row.get("目标表类型", "")
        target_table_type = "hive"
        if not pd.isna(target_table_type_raw):
            tt = str(target_table_type_raw).strip().lower()
            if tt and tt != "nan":
                if tt in {"hive", "clickhouse"}:
                    target_table_type = tt
                else:
                    logger.warning("未知目标表类型 '%s'，表 %s 将使用默认 hive 类型", target_table_type_raw, raw_table_name)

        op_type_raw = t_row.get("操作类型", "")
        op_type = "create"
        if not pd.isna(op_type_raw):
            v = str(op_type_raw).strip()
            if v == "修改表":
                op_type = "alter"

        hive_table_name_from_excel = t_row.get("hive表名", "")
        has_hive_table_name = (
            not pd.isna(hive_table_name_from_excel)
            and str(hive_table_name_from_excel).strip()
            and str(hive_table_name_from_excel).strip().lower() != "nan"
        )

        if not raw_table_name and not has_hive_table_name:
            logger.warning("表名为空且hive表名也为空，跳过该行")
            continue

        if not raw_table_name and has_hive_table_name:
            raw_table_name = str(hive_table_name_from_excel).strip()
            logger.info("表名为空，但hive表名有值，使用hive表名 '%s' 作为匹配字段", raw_table_name)

        if has_hive_table_name:
            hive_table_name = str(hive_table_name_from_excel).strip()
            logger.info(
                "开始处理表: 原表名=%s, 产品线=%s, 入仓方式=%s, Hive表名=%s (来自Excel)",
                raw_table_name,
                product_line,
                load_type,
                hive_table_name,
            )
        else:
            hive_table_name = build_table_name(raw_table_name, product_line, load_type)
            logger.info(
                "开始处理表: 原表名=%s, 产品线=%s, 入仓方式=%s, Hive表名=%s (自动拼接)",
                raw_table_name,
                product_line,
                load_type,
                hive_table_name,
            )

        if has_hive_table_name:
            match_table_name = str(hive_table_name_from_excel).strip()
            logger.debug("使用hive表名 '%s' 匹配 fields sheet 中的表名", match_table_name)
        else:
            match_table_name = raw_table_name
            logger.debug("使用原始表名 '%s' 匹配 fields sheet 中的表名", match_table_name)

        fields_df_normalized = fields_df.copy()
        fields_df_normalized["表名_标准化"] = fields_df_normalized["表名"].apply(
            lambda x: str(x).strip() if not pd.isna(x) else ""
        )

        table_fields_df = fields_df_normalized[fields_df_normalized["表名_标准化"] == match_table_name]
        if table_fields_df.empty:
            logger.warning(
                "表 %s (匹配字段: %s) 在 fields sheet 中未找到任何字段定义，跳过生成",
                raw_table_name,
                match_table_name,
            )
            available_table_names = fields_df_normalized["表名_标准化"].unique()
            logger.debug("fields sheet 中可用的表名: %s", list(available_table_names))
            continue

        table_fields_df = fields_df.loc[table_fields_df.index]

        def _normalize_field_op(v):
            if pd.isna(v):
                return "create"
            s = str(v).strip()
            return "alter" if s == "修改表" else "create"

        if "操作类型" in table_fields_df.columns:
            table_fields_df = table_fields_df.copy()
            table_fields_df["操作类型_标准化"] = table_fields_df["操作类型"].apply(_normalize_field_op)
        else:
            table_fields_df = table_fields_df.copy()
            table_fields_df["操作类型_标准化"] = "create"

        if target_table_type == "clickhouse" and op_type == "alter":
            new_fields_df = table_fields_df[table_fields_df["操作类型_标准化"] == "alter"]
            if new_fields_df.empty:
                logger.warning(
                    "表 %s (hive表名=%s) 为修改表且目标表类型为 clickhouse，但未找到任何标记为“修改表”的字段，跳过生成",
                    raw_table_name,
                    hive_table_name,
                )
                continue

            create_sql = build_alter_table_sql_clickhouse(hive_table_name, new_fields_df)
            output_path = output_dir / f"{hive_table_name}_ck_alter.sql"
        elif target_table_type == "clickhouse":
            create_sql = build_create_table_sql_clickhouse(hive_table_name, table_fields_df)
            output_path = output_dir / f"{hive_table_name}_ck.sql"
        elif target_table_type == "hive" and op_type == "alter":
            new_fields_df = table_fields_df[table_fields_df["操作类型_标准化"] == "alter"]
            if new_fields_df.empty:
                logger.warning(
                    "表 %s (hive表名=%s) 为修改表且目标表类型为 hive，但未找到任何标记为「修改表」的字段，跳过生成",
                    raw_table_name,
                    hive_table_name,
                )
                continue

            create_sql = build_alter_table_sql_hive(hive_table_name, new_fields_df)
            output_path = output_dir / f"{hive_table_name}_hive_alter.sql"
        else:
            create_sql = build_create_table_sql(
                hive_table_name,
                table_comment,
                table_fields_df,
                load_type,
                storage_format=storage_format,
                dw_layer=dw_layer,
            )
            output_path = output_dir / f"{hive_table_name}_hive.sql"

        if os.path.exists(output_path):
            logger.info("检测到已存在文件，将先删除再生成: %s", output_path)
            os.remove(output_path)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(create_sql)
        logger.info("表 %s 的建表语句已写入: %s", hive_table_name, output_path)

        if op_type == "create":
            rpa_rows.append(
                build_rpa_row(
                    target_table_type=target_table_type,
                    create_sql=create_sql,
                    dw_layer=dw_layer,
                )
            )

    write_rpa_sheet(excel_path_for_rpa, rpa_rows)
    logger.info("rpa sheet 已写入: %s，共 %d 行", excel_path_for_rpa, len(rpa_rows))
    logger.info("所有表处理完成")


def main() -> None:
    args = parse_args()
    today = datetime.now().strftime("%Y%m%d")
    output_dir = args.output_dir if args.output_dir is not None else OUTPUT_BASE_DIR / today
    output_dir.mkdir(parents=True, exist_ok=True)
    setup_logging(debug=args.debug, log_dir=output_dir)
    logger.info("create_table 脚本启动")

    excel_path_for_rpa = output_dir / EXCEL_FILENAME

    try:
        if args.input_excel is not None:
            if not args.input_excel.is_file():
                logger.error("Excel 文件不存在: %s", args.input_excel)
                sys.exit(1)
            data = load_excel(str(args.input_excel))
        elif args.json_file is not None:
            if not args.json_file.is_file():
                logger.error("JSON 文件不存在: %s", args.json_file)
                sys.exit(1)
            data = load_json_input(args.json_file.read_text(encoding="utf-8"))
        else:
            assert args.json_string is not None
            data = load_json_input(args.json_string)
    except ValueError as e:
        logger.error("%s", e)
        sys.exit(1)

    tables_df = data["tables"]
    fields_df = data["fields"]
    logger.info("输出目录: %s", output_dir)

    run_generation(output_dir, excel_path_for_rpa, tables_df, fields_df)


if __name__ == "__main__":
    main()
