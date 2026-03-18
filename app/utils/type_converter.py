"""数据类型转换工具模块"""
import logging
import re
import sys
from pathlib import Path
from typing import Dict

# 将项目根目录添加到Python路径
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

logger = logging.getLogger(__name__)


def mysql_type_to_hive(mysql_type: str) -> str:
    """
    将 MySQL 字段类型转换为 Hive 字段类型。
    规则见需求文档。
    """
    if not mysql_type:
        return "STRING"

    t = mysql_type.strip().upper()

    # 先处理整数类型：带不带括号都统一为 BIGINT，例如 BIGINT(20)、INT(11)、TINYINT(4)
    int_match = re.match(r"^(BIGINT|TINYINT|INT|INTEGER)\b", t)
    if int_match:
        return "BIGINT"
    
    # 浮点类型统一转换为 STRING
    float_match = re.match(r"^(FLOAT|DOUBLE)\b", t)
    if float_match:
        return "STRING"
    
    # 精确数值类型（DECIMAL、NUMERIC）统一转换为 STRING
    decimal_match = re.match(r"^(DECIMAL|NUMERIC)\b", t)
    if decimal_match:
        return "STRING"

    # 其他全部映射为 STRING
    return "STRING"

def mysql_type_to_clickhouse(mysql_type: str) -> str:
    """
    将 MySQL 字段类型转换为 Clickhouse 字段类型。
    规则见需求文档。
    浮点类型也转换成String类型，是因为目前的使用场景是hive同步clickhouse，而hive都是String类型，所以clickhouse也同步设置为String类型
    """
    if not mysql_type:
        return "String"

    t = mysql_type.strip().upper()

    # 先处理整数类型：带不带括号都统一为 BIGINT，例如 BIGINT(20)、INT(11)、TINYINT(4)
    int_match = re.match(r"^(BIGINT|TINYINT|INT|INTEGER)\b", t)
    if int_match:
        return "Int64"
    
    # 浮点类型统一转换为 STRING
    float_match = re.match(r"^(FLOAT|DOUBLE)\b", t)
    if float_match:
        return "String"
    
    # 精确数值类型（DECIMAL、NUMERIC）统一转换为 STRING
    decimal_match = re.match(r"^(DECIMAL|NUMERIC)\b", t)
    if decimal_match:
        return "String"

    # 其他全部映射为 STRING
    return "String"
