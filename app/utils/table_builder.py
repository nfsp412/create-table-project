"""表构建工具模块"""
import logging
import sys
from pathlib import Path
from typing import Dict

import pandas as pd

# 将项目根目录添加到Python路径
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from app.utils.type_converter import mysql_type_to_hive, mysql_type_to_clickhouse

logger = logging.getLogger(__name__)


def normalize_field_comment(comment: str) -> str:
    """
    规范化字段注释：将特殊字符（换行符、制表符等）统一转换为单个空格。
    
    优化5：避免复制粘贴时带入的特殊字符导致建表语句出现换行错误。
    
    Args:
        comment: 原始字段注释字符串
        
    Returns:
        规范化后的字段注释（特殊字符替换为单个空格，多个连续空格合并为一个）
    """
    if not comment:
        return ""
    
    import re
    # 将所有空白字符（换行符、制表符、回车符等）替换为单个空格
    normalized = re.sub(r'\s+', ' ', str(comment))
    # 去除首尾空格
    normalized = normalized.strip()
    return normalized


def _is_auto_inc_id(col_name: str, col_comment: str) -> bool:
    """
    通过字段名 + 字段注释综合判断是否为“自增ID/主键”字段。
    """
    name_l = str(col_name or "").lower()
    comment_l = str(col_comment or "").lower()

    # 字段名命中：常见 id / pk / key 相关模式
    name_keywords = ["id", "_id", "pk", "key"]
    name_hit = any(k in name_l for k in name_keywords)

    # 注释命中：自增、主键等中英文关键字
    comment_keywords = ["自增", "主键", "自增长", "identity", "auto_increment"]
    comment_hit = any(k in comment_l for k in comment_keywords)

    # 只有同时命中字段名和注释时才认为是自增ID，避免误判
    return bool(name_hit and comment_hit)


def normalize_load_type(load_type: str) -> str:
    """
    入仓方式转换为 hive 表名中使用的 day / hour。
    """
    if not load_type:
        return "unknown"
    lt = str(load_type).strip().lower()
    if "天" in lt or "day" in lt:
        return "day"
    if "小时" in lt or "hour" in lt:
        return "hour"
    return lt


def build_table_name(raw_table_name: str, product_line: str, load_type: str) -> str:
    """
    构建Hive表名：ods_产品线_表名_入仓方式
    """
    hive_load = normalize_load_type(load_type)
    # 简单规范：全部转小写，去掉首尾空格
    t = str(raw_table_name).strip().lower()
    p = str(product_line).strip().lower()
    return f"ods_ad_{p}_{t}_{hive_load}"


def build_create_table_sql(
    hive_table_name: str,
    table_comment: str,
    fields_df: pd.DataFrame,
    load_type: str = "",
    storage_format: str = "rcfile",
    dw_layer: str = "ods",
) -> str:
    """
    根据单个表的字段信息生成 Hive 建表语句。
    期望 fields_df 已经过滤为当前表的所有字段行。
    
    Args:
        hive_table_name: Hive 表名
        table_comment: 表注释
        fields_df: 字段信息 DataFrame
        load_type: 入仓方式（天表/小时表），用于在表注释后补充
        storage_format: 建表存储格式，来自 Excel「建表格式」列，可选 text/orc/rcfile，默认 orc
        dw_layer: 数仓分层，用于 LOCATION 路径 viewfs://c9/dw/{dw_layer}/{表名}，为空时默认 ods
    """
    columns_sql = []
    for _, row in fields_df.iterrows():
        col_name = str(row["字段名"]).strip()
        mysql_type = str(row["字段数据类型"]).strip() if not pd.isna(row["字段数据类型"]) else ""
        # 优化5：规范化字段注释，将特殊字符转换为单个空格
        raw_comment = "" if pd.isna(row["字段注释"]) else str(row["字段注释"])
        col_comment = normalize_field_comment(raw_comment).replace("'", "\\'")

        hive_type = mysql_type_to_hive(mysql_type)
        logger.debug(
            "字段解析: 表=%s, 字段名=%s, MySQL类型=%s, Hive类型=%s, 字段注释=%s",
            hive_table_name,
            col_name,
            mysql_type,
            hive_type,
            col_comment,
        )
        col_def = f"  `{col_name}` {hive_type}"
        if col_comment:
            col_def += f" COMMENT '{col_comment}'"
        columns_sql.append(col_def)

    columns_block = ",\n".join(columns_sql)

    # 处理表注释：根据入仓方式补充"天表"或"小时表"
    table_comment_clean = "" if pd.isna(table_comment) else str(table_comment).replace("'", "\\'")
    
    # 根据入仓方式补充"天表"或"小时表"
    if load_type:
        load_type_normalized = normalize_load_type(load_type)
        if load_type_normalized == "day":
            load_type_suffix = "天表"
        elif load_type_normalized == "hour":
            load_type_suffix = "小时表"
        else:
            load_type_suffix = ""
        
        if load_type_suffix:
            if table_comment_clean:
                table_comment_clean = f"{table_comment_clean}{load_type_suffix}"
            else:
                table_comment_clean = load_type_suffix
    
    table_comment_clause = f"COMMENT '{table_comment_clean}'" if table_comment_clean else ""

    # 根据入仓方式决定分区字段：
    # - 天表：仅按 dt 分区（保持不变）
    # - 小时表：按 dt、hour 双分区
    partition_clause = "PARTITIONED BY (`dt` string)"
    if load_type and normalize_load_type(load_type) == "hour":
        partition_clause = "PARTITIONED BY (`dt` string, `hour` string)"

    # 构建通用的分区 & 行格式块
    base_block = f"""{partition_clause}
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
LINES TERMINATED BY '\\n'
NULL DEFINED AS ''
"""
    # 数仓分层：空时默认 ods，用于 LOCATION 'viewfs://c9/dw/{dw_layer}/{hive表名}'
    layer = (dw_layer or "").strip().lower() or "ods"
    location_block = f"LOCATION 'viewfs://c9/dw/{layer}/${{hive表名}}'"
    
    # 建表格式：text / orc / rcfile，默认 rcfile
    fmt = (storage_format or "").strip().lower() or "rcfile"
    if fmt == "rcfile":
        # RCFILE 格式不需要 ORC 的 TBLPROPERTIES
        extra_block = base_block + "STORED AS RCFILE  \n" + location_block
        logger.debug("使用 RCFILE 格式构建表 %s", hive_table_name)
    elif fmt == "text":
        # TEXT 格式：不加 ORC 相关属性
        extra_block = base_block + "STORED AS TEXTFILE  \n" + location_block
        logger.debug("使用 TEXTFILE 格式构建表 %s", hive_table_name)
    else:
        # 默认 ORC
        extra_block = base_block + "STORED AS ORC  \nTBLPROPERTIES('orc.compress'='SNAPPY')\n" + location_block
        logger.debug("使用 ORC 格式构建表 %s", hive_table_name)

    # 将占位符替换为真实表名
    extra_block = extra_block.replace("${hive表名}", hive_table_name)

    # 使用 default 数据库，并对库名和表名都加反引号
    create_sql = f"CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`{hive_table_name}` (\n{columns_block}\n)"
    if table_comment_clause:
        create_sql += f"\n{table_comment_clause}"
    create_sql += f"\n{extra_block};"

    return create_sql

def build_create_table_sql_clickhouse(
    hive_table_name: str,
    fields_df: pd.DataFrame,
) -> str:
    """
    根据单个表的字段信息生成 Clickhouse 建表语句。
    期望 fields_df 已经过滤为当前表的所有字段行。
    分为本地表和分布式表两个建表语句
    
    Args:
        hive_table_name: Hive 表名
        fields_df: 字段信息 DataFrame
    """
    columns_sql = []
    candidate_id_field = None

    for _, row in fields_df.iterrows():
        col_name = str(row["字段名"]).strip()
        mysql_type = str(row["字段数据类型"]).strip() if not pd.isna(row["字段数据类型"]) else ""
        # 优化5：规范化字段注释，将特殊字符转换为单个空格
        raw_comment = "" if pd.isna(row["字段注释"]) else str(row["字段注释"])
        col_comment = normalize_field_comment(raw_comment).replace("'", "\\'")
        
        # 识别自增ID/主键字段（优先使用首个命中字段）
        if candidate_id_field is None and _is_auto_inc_id(col_name, col_comment):
            candidate_id_field = col_name

        clickhouse_type = mysql_type_to_clickhouse(mysql_type)
        logger.debug(
            "字段解析: 表=%s, 字段名=%s, MySQL类型=%s, Hive类型=%s, 字段注释=%s",
            hive_table_name,
            col_name,
            mysql_type,
            clickhouse_type,
            col_comment,
        )
        col_def = f"  `{col_name}` {clickhouse_type}"
        if col_comment:
            col_def += f" COMMENT '{col_comment}'"
        columns_sql.append(col_def)

    # clickhouse表需要追加一个dt字段
    columns_sql.append("  `dt` Date")
    columns_block = ",\n".join(columns_sql)

    # 构建 Clickhouse 分区 & 行格式块
    partition_clause = "PARTITION BY (dt)"

    # 根据是否存在自增ID字段决定 ORDER BY
    if candidate_id_field:
        order_by_clause = f"ORDER BY (`{candidate_id_field}`)"
        logger.debug(
            "ClickHouse 表 %s 使用字段 %s 作为 ORDER BY 键",
            hive_table_name,
            candidate_id_field,
        )
    else:
        order_by_clause = "ORDER BY ()"
        logger.debug(
            "ClickHouse 表 %s 未识别到自增ID字段，保持 ORDER BY ()",
            hive_table_name,
        )

    # 构建本地表sql和分布式表sql
    local_table_sql = f"""
CREATE TABLE `dplus_hubble`.`{hive_table_name}` ON CLUSTER logger (
{columns_block}
) 
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{hive_table_name}', '{{replica}}')
{partition_clause}
{order_by_clause}
TTL dt + toIntervalYear(2)
SETTINGS index_granularity = 8192
;
"""
    
    distributed_table_sql = f"""
CREATE TABLE `dplus_hubble`.`{hive_table_name}_cluster` (
{columns_block}
) 
ENGINE = Distributed('logger', 'dplus_hubble', '{hive_table_name}', rand())
;
"""

    return local_table_sql + distributed_table_sql


def build_alter_table_sql_clickhouse(
    hive_table_name: str,
    fields_df: pd.DataFrame,
) -> str:
    """
    根据需要新增的字段信息生成 Clickhouse 新增字段的 ALTER 语句。
    期望 fields_df 已经过滤为当前表的“需要新增”的字段行。
    对每个字段生成 add column 子句，并按表合并为：
    - 本地表一条 ALTER 语句
    - 分布式表一条 ALTER 语句
    字段注释为空时不输出列 COMMENT（与 Hive ALTER 一致）。
    """
    local_add_clauses = []
    dist_add_clauses = []

    for _, row in fields_df.iterrows():
        col_name = str(row["字段名"]).strip()
        mysql_type = str(row["字段数据类型"]).strip() if not pd.isna(row["字段数据类型"]) else ""
        raw_comment = "" if pd.isna(row["字段注释"]) else str(row["字段注释"])
        col_comment = normalize_field_comment(raw_comment).replace("'", "\\'")

        clickhouse_type = mysql_type_to_clickhouse(mysql_type)
        logger.debug(
            "ALTER 字段解析: 表=%s, 字段名=%s, MySQL类型=%s, ClickHouse类型=%s, 字段注释=%s",
            hive_table_name,
            col_name,
            mysql_type,
            clickhouse_type,
            col_comment,
        )
        # 根据 ClickHouse 字段类型选择合适的默认值：
        # - 整数类型（如 Int64）：DEFAULT 0
        # - 其他类型（当前映射为 String）：DEFAULT ''
        if str(clickhouse_type).startswith("Int"):
            default_literal = "0"
        else:
            default_literal = "''"

        clause = f"add column `{col_name}` {clickhouse_type} DEFAULT {default_literal}"
        if col_comment:
            clause += f" COMMENT '{col_comment}'"
        local_add_clauses.append(clause)
        dist_add_clauses.append(clause)

    if not local_add_clauses:
        return ""

    local_body = ",\n    ".join(local_add_clauses)
    dist_body = ",\n    ".join(dist_add_clauses)

    local_stmt = (
        f"-- 本地表\n"
        f"alter table dplus_hubble.{hive_table_name} on cluster logger\n"
        f"    {local_body};"
    )
    distributed_stmt = (
        f"-- 分布式表\n"
        f"alter table dplus_hubble.{hive_table_name}_cluster on cluster logger\n"
        f"    {dist_body};"
    )

    return f"{local_stmt}\n\n{distributed_stmt}"


def build_alter_table_sql_hive(
    hive_table_name: str,
    fields_df: pd.DataFrame,
) -> str:
    """
    根据需要新增的字段信息生成 Hive 新增字段的 ALTER 语句。
    期望 fields_df 已经过滤为当前表的"需要新增"的字段行。
    对每个字段生成 ADD COLUMNS 子句，合并为一条 ALTER TABLE 语句。
    字段注释为空时不输出列 COMMENT（与 Hive 新建表一致）。

    Args:
        hive_table_name: Hive 表名
        fields_df: 字段信息 DataFrame（需包含：字段名、字段数据类型、字段注释）

    Returns:
        Hive ALTER TABLE ADD COLUMNS 的 SQL 字符串
    """
    columns_sql = []

    for _, row in fields_df.iterrows():
        col_name = str(row["字段名"]).strip()
        mysql_type = str(row["字段数据类型"]).strip() if not pd.isna(row["字段数据类型"]) else ""
        raw_comment = "" if pd.isna(row["字段注释"]) else str(row["字段注释"])
        col_comment = normalize_field_comment(raw_comment).replace("'", "\\'")

        hive_type = mysql_type_to_hive(mysql_type)
        logger.debug(
            "ALTER 字段解析: 表=%s, 字段名=%s, MySQL类型=%s, Hive类型=%s, 字段注释=%s",
            hive_table_name,
            col_name,
            mysql_type,
            hive_type,
            col_comment,
        )
        col_def = f"  `{col_name}` {hive_type}"
        if col_comment:
            col_def += f" COMMENT '{col_comment}'"
        columns_sql.append(col_def)

    if not columns_sql:
        return ""

    columns_block = ",\n".join(columns_sql)
    alter_sql = (
        f"alter table default.`{hive_table_name}` add columns (\n"
        f"{columns_block}\n"
        f") cascade;"
    )
    return alter_sql