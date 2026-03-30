"""JSON 输入（原 add-sql-to-excel）对应的数据类。"""
from dataclasses import dataclass, field


@dataclass
class NewField:
    """修改表场景下待新增的字段。"""

    field_name: str
    field_type: str


@dataclass
class ModifyTableInput:
    """修改表 JSON 解析结果（无 mysql_sql）。"""

    table_name: str
    target_table_format: str
    operate_type: str
    new_fields: list[NewField] = field(default_factory=list)


@dataclass
class InputData:
    mysql_sql: str
    day_or_hour: str
    product_line: str
    dw_layer: str | None = None
    table_format: str | None = None
    target_table_format: str | None = None
    operate_type: str | None = None
    table_comment: str | None = None
    is_sharding: str = "否"
    hive_table_name: str | None = None
