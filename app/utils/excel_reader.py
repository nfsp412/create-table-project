"""Excel读取模块"""
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd
from tabulate import tabulate

# 将项目根目录添加到Python路径
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from app.utils.mysql_parser import parse_mysql_create_table
from app.utils.table_builder import normalize_field_comment

logger = logging.getLogger(__name__)


def load_excel(excel_path: str) -> Dict[str, pd.DataFrame]:
    """
    读取Excel文件，返回tables和fields两个DataFrame。
    
    Args:
        excel_path: Excel 文件的完整路径
    
    fields sheet统一使用混合格式（5列）：
    - 表名（必需）
    - 字段名
    - 字段数据类型
    - 字段注释
    - 建表语句
    
    处理规则（逐行检测）：
    - 当一行有"建表语句"且不为空时，解析建表语句提取字段信息
    - 当一行有"字段名"、"字段数据类型"、"字段注释"且不为空时，直接使用这些字段信息
    - 同一sheet中可以混合使用两种方式
    """
    logger.info("开始读取 Excel 文件: %s", excel_path)
    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"Excel 文件不存在: {excel_path}")

    xls = pd.ExcelFile(excel_path)
    tables_df = pd.read_excel(xls, "tables")
    fields_df = pd.read_excel(xls, "fields")
    logger.info("Excel 读取完成, tables 行数=%d, fields 行数=%d", len(tables_df), len(fields_df))
    logger.debug("tables 列信息: %s", list(tables_df.columns))
    logger.debug("fields 列信息: %s", list(fields_df.columns))

    # 验证fields sheet是否包含必需的5列
    fields_columns = list(fields_df.columns)
    required_columns = ["表名", "字段名", "字段数据类型", "字段注释", "建表语句"]
    missing_columns = [col for col in required_columns if col not in fields_columns]
    
    if missing_columns:
        raise ValueError(
            f"fields sheet 缺少必需的列: {missing_columns}。"
            f"fields sheet 必须包含以下5列: {required_columns}"
        )
    
    logger.info("fields sheet 包含5列（混合格式），将逐行检测处理方式")
    fields_df = process_fields_dataframe(fields_df)

    # 对齐美化 fields 前几行样例输出，确保所有列左对齐
    fields_preview_df = fields_df.head()
    # 使用 tabulate 格式化，确保所有列左对齐
    fields_preview = tabulate(
        fields_preview_df,
        headers=fields_preview_df.columns,
        tablefmt="grid",
        showindex=False,
        stralign="left",
        numalign="left",
    )
    logger.debug("fields 前几行样例:\n%s", fields_preview)
    return {"tables": tables_df, "fields": fields_df}


def process_fields_dataframe(fields_df: pd.DataFrame) -> pd.DataFrame:
    """
    处理 fields 原始 DataFrame：逐行检测处理方式并提取字段信息。
    
    处理规则（逐行检测）：
    - 如果一行有"建表语句"且不为空，解析建表语句提取字段信息
    - 如果一行有"字段名"、"字段数据类型"且不为空，直接使用这些字段信息（字段注释允许为空）
    - 同一sheet中可以混合使用两种方式
    
    Args:
        fields_df: 包含5列的DataFrame（表名、字段名、字段数据类型、字段注释、建表语句），
                   允许额外包含可选列（如“操作类型”）
        
    Returns:
        包含"表名"、"字段名"、"字段数据类型"、"字段注释"及可选"操作类型"列的DataFrame
    """
    parsed_fields = []
    create_statement_count = 0
    direct_fields_count = 0
    
    for idx, row in fields_df.iterrows():
        table_name = str(row["表名"]).strip() if not pd.isna(row["表名"]) else ""
        create_table_sql = str(row["建表语句"]).strip() if not pd.isna(row["建表语句"]) else ""
        field_name = str(row["字段名"]).strip() if not pd.isna(row["字段名"]) else ""
        field_type = str(row["字段数据类型"]).strip() if not pd.isna(row["字段数据类型"]) else ""
        # 优化5：规范化字段注释，将特殊字符转换为单个空格
        raw_field_comment = str(row["字段注释"]).strip() if not pd.isna(row["字段注释"]) else ""
        field_comment = normalize_field_comment(raw_field_comment)
        
        # 可选：字段级操作类型（例如：新建表 / 修改表），默认视为“新建表”
        field_op_raw = ""
        if "操作类型" in fields_df.columns:
            field_op_raw = "" if pd.isna(row.get("操作类型")) else str(row.get("操作类型")).strip()
        field_op = field_op_raw or "新建表"
        
        # 判断使用哪种处理方式
        has_create_statement = create_table_sql and create_table_sql.strip()
        # 允许字段注释为空：只要字段名与字段类型齐全，就视为直接字段信息
        has_direct_fields = field_name and field_type
        
        if has_create_statement:
            # 方式1：解析建表语句
            logger.debug("第 %d 行：表 %s 使用建表语句解析", idx + 2, table_name)  # +2 因为从0开始且包含表头
            try:
                fields_list = parse_mysql_create_table(create_table_sql)
                for field_info in fields_list:
                    field_info["表名"] = table_name
                    # 继承该行的操作类型，便于后续区分“新建表/修改表”字段
                    field_info["操作类型"] = field_op
                    parsed_fields.append(field_info)
                create_statement_count += 1
            except Exception as e:
                logger.error("解析表 %s 的建表语句失败: %s", table_name, e)
                continue
        elif has_direct_fields:
            # 方式2：直接使用字段信息
            logger.debug("第 %d 行：表 %s 使用直接字段信息", idx + 2, table_name)
            parsed_fields.append({
                "表名": table_name,
                "字段名": field_name,
                "字段数据类型": field_type,
                "字段注释": field_comment,
                "操作类型": field_op,
            })
            direct_fields_count += 1
        else:
            # 格式不明确，记录警告
            logger.warning("第 %d 行：表 %s 的处理方式不明确，既没有建表语句，也没有完整的字段信息，跳过", idx + 2, table_name)
            continue
    
    logger.info("fields sheet 处理完成：建表语句解析 %d 行，直接字段信息 %d 行，共生成 %d 个字段记录", 
                create_statement_count, direct_fields_count, len(parsed_fields))
    
    # 转换为DataFrame
    if parsed_fields:
        result_df = pd.DataFrame(parsed_fields)
        # 去重：同一表名+字段名只保留首次出现，避免同表多行（如 hive+clickhouse）导致重复字段
        before_count = len(result_df)
        result_df = result_df.drop_duplicates(subset=["表名", "字段名"], keep="first")
        if len(result_df) < before_count:
            logger.info("fields 去重：%d -> %d 条（同表多行建表语句已合并）", before_count, len(result_df))
        # 确保返回列的顺序稳定：若存在“操作类型”则一并返回
        cols = ["表名", "字段名", "字段数据类型", "字段注释"]
        if "操作类型" in result_df.columns:
            cols.append("操作类型")
        return result_df[cols]
    else:
        logger.warning("未解析出任何字段信息")
        cols = ["表名", "字段名", "字段数据类型", "字段注释", "操作类型"]
        return pd.DataFrame(columns=cols)


def _process_fields_sheet(fields_df: pd.DataFrame) -> pd.DataFrame:
    """兼容旧名，等同于 process_fields_dataframe。"""
    return process_fields_dataframe(fields_df)
