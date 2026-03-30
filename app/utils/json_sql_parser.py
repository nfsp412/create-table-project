"""JSON 输入解析（与 add-sql-to-excel sql_parser 对齐）。"""
import json
import logging
import re
from typing import Union

from app.input_models import InputData, ModifyTableInput, NewField

logger = logging.getLogger(__name__)

CREATE_TABLE_REQUIRED_FIELDS = (
    "mysql_sql",
    "product_line",
    "day_or_hour",
    "dw_layer",
    "table_format",
    "target_table_format",
    "operate_type",
    "is_sharding",
)

DW_LAYER_VALUES = frozenset(("ods", "mds", "sds"))
TABLE_FORMAT_VALUES = frozenset(("orc", "rcfile", "text"))
TARGET_TABLE_FORMAT_VALUES = frozenset(("hive", "clickhouse"))
OPERATE_TYPE_VALUES = frozenset(("新建表", "修改表"))
IS_SHARDING_VALUES = frozenset(("是", "否"))

_ALL_KEYS = (
    "mysql_sql", "day_or_hour", "product_line",
    "dw_layer", "table_format", "target_table_format", "operate_type",
    "table_comment", "is_sharding", "table_name", "hive_table_name",
)


def _try_repair_json(raw: str) -> dict | None:
    """当 json.loads 因 SQL 中未转义的双引号失败时，利用已知 key 名作为锚点提取各字段值。"""
    key_pattern = "|".join(re.escape(k) for k in _ALL_KEYS)
    anchor_re = re.compile(rf'"({key_pattern})"\s*:\s*"')

    matches = list(anchor_re.finditer(raw))
    if not matches:
        return None

    result: dict[str, str] = {}
    for i, m in enumerate(matches):
        key = m.group(1)
        val_start = m.end()

        if i + 1 < len(matches):
            segment = raw[val_start:matches[i + 1].start()]
            end = re.search(r'"\s*,\s*$', segment)
            if not end:
                return None
            value = segment[:end.start()]
        else:
            segment = raw[val_start:]
            end = re.search(r'"\s*}\s*$', segment)
            if not end:
                return None
            value = segment[:end.start()]

        result[key] = value.replace('"', "'")

    return result


def _validate_optional_field(value: str | None, allowed: frozenset[str], field_name: str) -> bool:
    if not value:
        return True
    if value in allowed:
        return True
    logger.warning("可选字段 %s 非法值 '%s'，允许值: %s，跳过本次处理", field_name, value, sorted(allowed))
    return False


def _parse_modify_table_dict(data: dict) -> ModifyTableInput | None:
    required = ("table_name", "operate_type", "target_table_format", "new_fields")
    missing = [f for f in required if f not in data or data.get(f) in (None, "", [])]
    if missing:
        logger.warning("修改表 JSON 缺少必需字段或为空: %s，跳过本次处理", missing)
        return None

    table_name = str(data["table_name"]).strip()
    if not table_name:
        logger.warning("修改表 JSON table_name 为空，跳过本次处理")
        return None

    operate_type = str(data["operate_type"]).strip()
    if operate_type != "修改表":
        logger.warning("修改表 JSON operate_type 须为「修改表」，当前为 '%s'，跳过本次处理", operate_type)
        return None

    target_table_format = str(data["target_table_format"]).strip()
    if not _validate_optional_field(target_table_format, TARGET_TABLE_FORMAT_VALUES, "target_table_format"):
        return None

    raw_fields = data["new_fields"]
    if not isinstance(raw_fields, list) or len(raw_fields) == 0:
        logger.warning("修改表 JSON new_fields 须为非空数组，跳过本次处理")
        return None

    new_fields: list[NewField] = []
    for i, item in enumerate(raw_fields):
        if not isinstance(item, dict):
            logger.warning("修改表 new_fields[%d] 不是对象，跳过本次处理", i)
            return None
        fn = item.get("field_name")
        if not fn or not str(fn).strip():
            logger.warning("修改表 new_fields[%d] 缺少 field_name，跳过本次处理", i)
            return None
        ft_raw = item.get("field_type")
        ft = "string" if ft_raw in (None, "") else str(ft_raw).strip()
        new_fields.append(NewField(field_name=str(fn).strip(), field_type=ft))

    return ModifyTableInput(
        table_name=table_name,
        target_table_format=target_table_format,
        operate_type=operate_type,
        new_fields=new_fields,
    )


def _parse_create_table_dict(data: dict) -> InputData | None:
    missing = [f for f in CREATE_TABLE_REQUIRED_FIELDS if not data.get(f)]
    if missing:
        logger.warning("JSON 缺少必需字段: %s，跳过本次处理", missing)
        return None

    dw_layer = str(data["dw_layer"]).strip()
    table_format = str(data["table_format"]).strip()
    target_table_format = str(data["target_table_format"]).strip()
    operate_type = str(data["operate_type"]).strip()
    is_sharding = str(data["is_sharding"]).strip()

    if not _validate_optional_field(dw_layer, DW_LAYER_VALUES, "dw_layer"):
        return None
    if not _validate_optional_field(table_format, TABLE_FORMAT_VALUES, "table_format"):
        return None
    if not _validate_optional_field(target_table_format, TARGET_TABLE_FORMAT_VALUES, "target_table_format"):
        return None
    if not _validate_optional_field(operate_type, OPERATE_TYPE_VALUES, "operate_type"):
        return None
    if not _validate_optional_field(is_sharding, IS_SHARDING_VALUES, "is_sharding"):
        return None

    table_comment = data.get("table_comment")
    if table_comment is not None:
        table_comment = str(table_comment).strip() or None
    if not table_comment:
        table_comment = parse_table_comment(data["mysql_sql"])

    hive_table_name_raw = data.get("hive_table_name")
    hive_table_name: str | None = None
    if hive_table_name_raw is not None:
        h = str(hive_table_name_raw).strip()
        hive_table_name = h or None

    return InputData(
        mysql_sql=data["mysql_sql"],
        day_or_hour=data["day_or_hour"],
        product_line=data["product_line"],
        dw_layer=dw_layer,
        table_format=table_format,
        target_table_format=target_table_format,
        operate_type=operate_type,
        table_comment=table_comment,
        is_sharding=is_sharding,
        hive_table_name=hive_table_name,
    )


def parse_table_comment(sql: str) -> str | None:
    paren_pos = sql.rfind(")")
    if paren_pos == -1:
        return None
    suffix = sql[paren_pos + 1:]
    match = re.search(r"COMMENT\s*=?\s*['\"](.+?)['\"]", suffix, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def strip_sharding_suffix(table_name: str) -> str:
    return re.sub(r"_\d+$", "", table_name)


def parse_table_name(sql: str) -> str | None:
    match = re.search(r"CREATE\s+TABLE\s+`?(\w+)`?", sql, re.IGNORECASE)
    if match:
        return match.group(1)
    logger.warning("无法从 SQL 中解析表名，SQL 片段: %.100s", sql)
    return None


def parse_input_dict(data: dict) -> Union[InputData, ModifyTableInput, None]:
    """按 `operate_type` 区分新建表 / 修改表，不再根据是否出现 `new_fields` 键自动选分支。"""
    ot_raw = data.get("operate_type")
    if ot_raw is None or (isinstance(ot_raw, str) and not str(ot_raw).strip()):
        logger.warning("JSON 缺少 operate_type（须为「新建表」或「修改表」），跳过本次处理")
        return None
    ot = str(ot_raw).strip()

    if ot == "修改表":
        nf = data.get("new_fields")
        if nf is None or (isinstance(nf, list) and len(nf) == 0):
            logger.warning("修改表 JSON 须包含非空 new_fields，跳过本次处理")
            return None
        return _parse_modify_table_dict(data)

    if ot == "新建表":
        ms = data.get("mysql_sql")
        if ms is None or not str(ms).strip():
            logger.warning("新建表 JSON 须包含非空 mysql_sql，跳过本次处理")
            return None
        return _parse_create_table_dict(data)

    logger.warning("operate_type 非法: '%s'，允许值: 新建表、修改表，跳过本次处理", ot)
    return None


def parse_json_to_items(json_str: str) -> list[Union[InputData, ModifyTableInput]]:
    """
    解析 JSON 字符串为若干 InputData / ModifyTableInput；无法解析或全部跳过则抛 ValueError。
    """
    try:
        parsed = json.loads(json_str)
    except json.JSONDecodeError:
        parsed = _try_repair_json(json_str)
        if parsed is None:
            raise ValueError("JSON 解析失败且自动修复未成功") from None
        logger.info("已自动修复 JSON（SQL 中的双引号已替换为单引号）")

    if isinstance(parsed, dict):
        items = [parsed]
    elif isinstance(parsed, list):
        items = parsed
    else:
        raise ValueError("输入格式错误，应为 JSON 对象或数组")

    out: list[Union[InputData, ModifyTableInput]] = []
    for i, item in enumerate(items):
        if not isinstance(item, dict):
            logger.warning("第 %d 项不是对象，跳过", i + 1)
            continue
        d = parse_input_dict(item)
        if d:
            out.append(d)
    if not out:
        raise ValueError("没有可解析的数据")
    return out
