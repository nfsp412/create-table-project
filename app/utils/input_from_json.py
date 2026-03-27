"""将 JSON 解析结果转为与 Excel 等价的 tables/fields DataFrame（内存路径，不落盘）。"""
import logging
from typing import Union

import pandas as pd

from app.input_models import InputData, ModifyTableInput
from app.utils.excel_reader import process_fields_dataframe
from app.utils.json_sql_parser import parse_json_to_items, parse_table_name, strip_sharding_suffix

logger = logging.getLogger(__name__)

TABLES_HEADERS = [
    "表名", "产品线", "入仓方式", "表注释信息",
    "数仓分层", "建表格式", "目标表类型", "操作类型", "hive表名",
    "是否分库分表",
]

FIELDS_HEADERS = [
    "表名", "字段名", "字段数据类型", "字段注释", "操作类型", "建表语句",
]


def _row_from_data(data: InputData) -> tuple[list, list] | None:
    table_name = parse_table_name(data.mysql_sql)
    if table_name is None:
        logger.warning("SQL 解析失败，跳过该项。")
        return None

    display_table_name = strip_sharding_suffix(table_name) if data.is_sharding == "是" else table_name

    tables_row = [
        display_table_name,
        data.product_line,
        data.day_or_hour,
        data.table_comment,
        data.dw_layer,
        data.table_format,
        data.target_table_format,
        data.operate_type,
        None,
        data.is_sharding,
    ]
    fields_row = [
        display_table_name,
        None,
        None,
        None,
        data.operate_type,
        data.mysql_sql,
    ]
    return (tables_row, fields_row)


def _rows_from_modify(data: ModifyTableInput) -> tuple[list, list[list]]:
    name = data.table_name.strip()
    tables_row = [
        name,
        None,
        None,
        None,
        None,
        None,
        data.target_table_format,
        data.operate_type,
        name,
        None,
    ]
    fields_rows = []
    for nf in data.new_fields:
        fields_rows.append(
            [
                name,
                nf.field_name,
                nf.field_type,
                None,
                data.operate_type,
                None,
            ]
        )
    return tables_row, fields_rows


def json_items_to_dataframes(
    items: list[Union[InputData, ModifyTableInput]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """将已解析的 InputData / ModifyTableInput 列表转为原始 tables、fields DataFrame（未 process_fields）。"""
    tables_rows: list[list] = []
    fields_rows: list[list] = []

    for data in items:
        if isinstance(data, ModifyTableInput):
            t_row, f_rows = _rows_from_modify(data)
            tables_rows.append(t_row)
            fields_rows.extend(f_rows)
        else:
            rows = _row_from_data(data)
            if rows:
                tables_rows.append(rows[0])
                fields_rows.append(rows[1])

    tables_df = pd.DataFrame(tables_rows, columns=TABLES_HEADERS)
    fields_df = pd.DataFrame(fields_rows, columns=FIELDS_HEADERS)
    return tables_df, fields_df


def load_json_input(json_str: str) -> dict[str, pd.DataFrame]:
    """解析 JSON 字符串，返回与 load_excel 相同结构的 dict（fields 已 process_fields）。"""
    items = parse_json_to_items(json_str)
    tables_df, fields_raw = json_items_to_dataframes(items)
    fields_df = process_fields_dataframe(fields_raw)
    return {"tables": tables_df, "fields": fields_df}
