#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
主程序入口模块
Hive建表语句生成工具
"""
import argparse
import logging
import os
import sys
from pathlib import Path

import pandas as pd

# 智能设置项目根目录路径，支持从不同位置运行
# 从当前文件位置向上查找项目根目录（包含 setup.py 或 requirements.txt 的目录）
_current_file = Path(__file__).resolve()
project_root = _current_file.parent.parent  # app/main.py -> 向上2级

# 验证项目根目录（检查是否存在 setup.py 或 requirements.txt）
if not (project_root / "setup.py").exists() and not (project_root / "requirements.txt").exists():
    # 如果向上2级不是项目根目录，尝试从当前工作目录查找
    cwd = Path.cwd()
    if (cwd / "setup.py").exists() or (cwd / "requirements.txt").exists():
        project_root = cwd

# 将项目根目录添加到Python路径（如果尚未添加）
project_root_str = str(project_root.resolve())
if project_root_str not in sys.path:
    sys.path.insert(0, project_root_str)

from datetime import datetime

from app.config.settings import EXCEL_FILENAME, OUTPUT_BASE_DIR
from app.utils.excel_reader import load_excel
from app.utils.logger import setup_logging
from app.utils.table_builder import (
    build_create_table_sql,
    build_table_name,
    build_create_table_sql_clickhouse,
    build_alter_table_sql_clickhouse,
)

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """
    解析命令行参数
    """
    parser = argparse.ArgumentParser(description="根据 Excel 配置生成 Hive 建表语句")
    parser.add_argument(
        "--debug",
        action="store_true",
        help="开启 DEBUG 日志等级，输出更详细的调试信息",
    )
    return parser.parse_args()


def main() -> None:
    """
    主函数
    """
    args = parse_args()
    setup_logging(debug=args.debug)
    logger.info("create_table 脚本启动")

    today = datetime.now().strftime("%Y%m%d")
    output_dir = str(OUTPUT_BASE_DIR / today)
    excel_path = str(OUTPUT_BASE_DIR / today / EXCEL_FILENAME)

    data = load_excel(excel_path)
    tables_df = data["tables"]
    fields_df = data["fields"]
    os.makedirs(output_dir, exist_ok=True)
    logger.info("输出目录: %s", output_dir)

    # 期望 sheet 表头：
    # tables: 表名, 产品线, 入仓方式, 表注释信息, 数仓分层（可选）, 建表格式（可选）, hive表名（可选）, 目标表类型（可选）, 操作类型（可选：新建表/修改表）
    # fields: 统一使用混合格式（5列+可选“操作类型”列）
    #   - 表名, 字段名, 字段数据类型, 字段注释, 建表语句, （可选）操作类型
    #   处理规则：逐行检测，有建表语句则解析，有字段信息则直接使用

    for _, t_row in tables_df.iterrows():
        # 标准化处理表名：去除空格，处理NaN
        raw_table_name = str(t_row["表名"]).strip() if not pd.isna(t_row["表名"]) else ""
        
        product_line = t_row["产品线"]
        load_type = t_row["入仓方式"]
        table_comment = t_row.get("表注释信息", "")
        # 数仓分层：用于 LOCATION viewfs://c9/dw/{数仓分层}/{表名}，为空时默认 ods
        dw_layer_raw = t_row.get("数仓分层", "")
        dw_layer = "ods"
        if not pd.isna(dw_layer_raw) and str(dw_layer_raw).strip() and str(dw_layer_raw).strip().lower() != "nan":
            dw_layer = str(dw_layer_raw).strip().lower()
        
        # 建表格式：text / orc / rcfile，来自 tables 页「建表格式」列，默认 rcfile
        storage_format_raw = t_row.get("建表格式", "")
        storage_format = "rcfile"
        if not pd.isna(storage_format_raw):
            sf = str(storage_format_raw).strip().lower()
            if sf and sf != "nan":
                if sf in {"text", "orc", "rcfile"}:
                    storage_format = sf
                else:
                    logger.warning("未知建表格式 '%s'，表 %s 将使用默认 RCFILE 格式", storage_format_raw, raw_table_name)

        # 新增目标表类型（可选），默认是hive类型，可选：hive，clickhouse
        target_table_type_raw = t_row.get("目标表类型", "")
        target_table_type = "hive"
        if not pd.isna(target_table_type_raw):
            tt = str(target_table_type_raw).strip().lower()
            if tt and tt != "nan":
                if tt in {"hive", "clickhouse"}:
                    target_table_type = tt
                else:
                    logger.warning("未知目标表类型 '%s'，表 %s 将使用默认 hive 类型", target_table_type_raw, raw_table_name)

        # 表级操作类型（可选）：新建表 / 修改表，默认视为新建表
        op_type_raw = t_row.get("操作类型", "")
        op_type = "create"
        if not pd.isna(op_type_raw):
            v = str(op_type_raw).strip()
            if v == "修改表":
                op_type = "alter"
        
        # 优化2: 优先使用Excel中的"hive表名"，如果为空则使用拼接逻辑
        # 注意：pandas读取空单元格时返回NaN（float类型），需要正确处理
        hive_table_name_from_excel = t_row.get("hive表名", "")
        # 检查是否为NaN或空值：先检查是否为NaN，再检查转换为字符串后是否为空
        has_hive_table_name = (
            not pd.isna(hive_table_name_from_excel) 
            and str(hive_table_name_from_excel).strip() 
            and str(hive_table_name_from_excel).strip().lower() != 'nan'
        )
        
        # 如果表名为空，但hive表名有值，可以使用hive表名；如果两者都为空，则跳过
        if not raw_table_name and not has_hive_table_name:
            logger.warning("表名为空且hive表名也为空，跳过该行")
            continue
        
        # 如果表名为空但hive表名有值，使用hive表名作为原始表名（用于匹配fields页）
        if not raw_table_name and has_hive_table_name:
            raw_table_name = str(hive_table_name_from_excel).strip()
            logger.info("表名为空，但hive表名有值，使用hive表名 '%s' 作为匹配字段", raw_table_name)
        
        if has_hive_table_name:
            # 如果Excel中指定了hive表名，直接使用
            hive_table_name = str(hive_table_name_from_excel).strip()
            logger.info(
                "开始处理表: 原表名=%s, 产品线=%s, 入仓方式=%s, Hive表名=%s (来自Excel)",
                raw_table_name,
                product_line,
                load_type,
                hive_table_name,
            )
        else:
            # 如果Excel中未指定hive表名，使用拼接逻辑
            hive_table_name = build_table_name(raw_table_name, product_line, load_type)
            logger.info(
                "开始处理表: 原表名=%s, 产品线=%s, 入仓方式=%s, Hive表名=%s (自动拼接)",
                raw_table_name,
                product_line,
                load_type,
                hive_table_name,
            )

        # 修复匹配逻辑：如果tables页有hive表名，使用hive表名匹配fields页的表名；否则使用原始表名
        # 同时需要对表名进行标准化处理（去除空格、处理NaN）
        if has_hive_table_name:
            # 使用hive表名进行匹配
            match_table_name = str(hive_table_name_from_excel).strip()
            logger.debug("使用hive表名 '%s' 匹配 fields sheet 中的表名", match_table_name)
        else:
            # 使用原始表名进行匹配（已标准化处理）
            match_table_name = raw_table_name
            logger.debug("使用原始表名 '%s' 匹配 fields sheet 中的表名", match_table_name)
        
        # 对fields_df的表名也进行标准化处理后再匹配
        # 创建一个标准化的表名列用于匹配
        fields_df_normalized = fields_df.copy()
        fields_df_normalized["表名_标准化"] = fields_df_normalized["表名"].apply(
            lambda x: str(x).strip() if not pd.isna(x) else ""
        )
        
        table_fields_df = fields_df_normalized[fields_df_normalized["表名_标准化"] == match_table_name]
        if table_fields_df.empty:
            logger.warning("表 %s (匹配字段: %s) 在 fields sheet 中未找到任何字段定义，跳过生成", 
                          raw_table_name, match_table_name)
            # 输出调试信息：显示fields页中所有可用的表名
            available_table_names = fields_df_normalized["表名_标准化"].unique()
            logger.debug("fields sheet 中可用的表名: %s", list(available_table_names))
            continue
        
        # 使用原始fields_df（不包含标准化列）
        table_fields_df = fields_df.loc[table_fields_df.index]

        # 规范化字段级操作类型（可选），用于 ClickHouse 修改表场景
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

        # 建表格式：来自 tables 页「建表格式」列，优先级高于旧逻辑
        # 数仓分层：来自 tables 页「数仓分层」列，空时默认 ods，用于 LOCATION
        # 目标表类型 + 表级操作类型 决定走建表还是修改表逻辑
        if target_table_type == "clickhouse" and op_type == "alter":
            # 仅对字段级操作类型为“修改表”的字段生成新增字段 SQL
            new_fields_df = table_fields_df[table_fields_df["操作类型_标准化"] == "alter"]
            if new_fields_df.empty:
                logger.warning(
                    "表 %s (hive表名=%s) 为修改表且目标表类型为 clickhouse，但未找到任何标记为“修改表”的字段，跳过生成",
                    raw_table_name,
                    hive_table_name,
                )
                continue

            create_sql = build_alter_table_sql_clickhouse(
                hive_table_name,
                new_fields_df,
            )
            output_path = os.path.join(output_dir, f"{hive_table_name}_ck_alter.sql")
        elif target_table_type == "clickhouse":
            create_sql = build_create_table_sql_clickhouse(
                hive_table_name,
                table_fields_df,
            )
            output_path = os.path.join(output_dir, f"{hive_table_name}_ck.sql")
        else:
            create_sql = build_create_table_sql(
                hive_table_name,
                table_comment,
                table_fields_df,
                load_type,
                storage_format=storage_format,
                dw_layer=dw_layer,
            )
            output_path = os.path.join(output_dir, f"{hive_table_name}.sql")

        # 如果目标文件已存在，先删除再生成，保证是本次最新内容
        if os.path.exists(output_path):
            logger.info("检测到已存在文件，将先删除再生成: %s", output_path)
            os.remove(output_path)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(create_sql)
        logger.info("表 %s 的建表语句已写入: %s", hive_table_name, output_path)

    logger.info("所有表处理完成")


if __name__ == "__main__":
    main()
