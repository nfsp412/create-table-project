"""MySQL建表语句解析模块"""
import logging
import re
import sys
from pathlib import Path
from typing import Dict, List

# 将项目根目录添加到Python路径
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from app.utils.table_builder import normalize_field_comment

logger = logging.getLogger(__name__)

# MySQL / Hive 建表语句头部：支持 EXTERNAL、IF NOT EXISTS、TEMPORARY
CREATE_DDL_HEADER_RE = re.compile(
    r"CREATE\s+(?:TEMPORARY\s+)?(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?",
    re.IGNORECASE | re.DOTALL,
)


def _parse_table_identifier_after_header(sql: str, header_end: int) -> tuple[str | None, int | None]:
    """
    跳过 CREATE ... TABLE ... 头部之后到列定义括号前的表名。
    支持：`db`.`tbl`、`tbl`、db.tbl、t（限定名取最后一段作为逻辑表名）。
    返回 (逻辑表名, 表名结束后的下标)；无法识别时 (None, None)。
    """
    rest_slice = sql[header_end:]
    rest = rest_slice.lstrip()
    pos = header_end + (len(rest_slice) - len(rest))
    m_bt = re.match(r"`([^`]+)`(?:\s*\.\s*`([^`]+)`)?", sql[pos:])
    if m_bt:
        raw = m_bt.group(2) if m_bt.group(2) else m_bt.group(1)
        name = (raw or "").strip() or None
        return (name, pos + m_bt.end())
    m_uq = re.match(r"(\w+)(?:\.(\w+))?(?=\s*[(]|\s|$)", sql[pos:], re.DOTALL)
    if m_uq:
        g2 = m_uq.group(2)
        g1 = m_uq.group(1)
        raw = g2 if g2 else g1
        name = (raw or "").strip() or None
        return (name, pos + m_uq.end())
    return (None, None)


def parse_create_ddl_table_name(sql: str) -> str | None:
    """
    从 MySQL 或 Hive 风格的 CREATE [EXTERNAL] TABLE 语句中解析逻辑表名。
    `` `db`.`tbl` `` 时返回 `` tbl ``；用于 JSON 中 mysql_sql 填写 Hive DDL 的场景。
    """
    if not sql or not str(sql).strip():
        return None
    s = sql.strip()
    hm = CREATE_DDL_HEADER_RE.search(s)
    if not hm:
        logger.warning("无法从 SQL 中解析表名（未识别 CREATE [EXTERNAL] TABLE），SQL 片段: %.100s", s)
        return None
    name, end_pos = _parse_table_identifier_after_header(s, hm.end())
    if not name or end_pos is None:
        logger.warning("无法从 SQL 中解析表名（表名格式未识别），SQL 片段: %.100s", s)
        return None
    return name


def parse_mysql_create_table(create_table_sql: str) -> List[Dict[str, str]]:
    """
    解析 MySQL CREATE TABLE 语句，提取字段信息。
    
    返回字段列表，每个字段包含：
    - 字段名
    - 字段数据类型（MySQL类型）
    - 字段注释
    
    Args:
        create_table_sql: MySQL CREATE TABLE 语句字符串
        
    Returns:
        字段信息列表，格式：[{"字段名": "...", "字段数据类型": "...", "字段注释": "..."}, ...]
    """
    if not create_table_sql:
        logger.warning("建表语句为空")
        return []
    
    # 清理SQL语句：统一空白字符，但保留必要的空格
    sql = create_table_sql.strip()
    
    # 提取列定义括号：与 parse_create_ddl_table_name 一致，支持 Hive EXTERNAL / IF NOT EXISTS
    hm = CREATE_DDL_HEADER_RE.search(sql)
    if not hm:
        logger.warning("未找到 CREATE TABLE 关键字: %s", sql[:100])
        return []

    _tname, end_after_name = _parse_table_identifier_after_header(sql, hm.end())
    if end_after_name is None:
        logger.warning("未找到表名: %s", sql[:100])
        return []

    start_pos = end_after_name
    # 找到第一个 (
    first_paren = sql.find('(', start_pos)
    if first_paren == -1:
        logger.warning("未找到字段定义开始括号: %s", sql[:100])
        return []
    
    # 从第一个 ( 开始，找到匹配的最后一个 )
    depth = 0
    end_pos = first_paren
    for i in range(first_paren, len(sql)):
        if sql[i] == '(':
            depth += 1
        elif sql[i] == ')':
            depth -= 1
            if depth == 0:
                end_pos = i
                break
    
    if depth != 0:
        logger.warning("括号不匹配: %s", sql[:100])
        return []
    
    columns_block = sql[first_paren + 1:end_pos]
    
    logger.debug("提取的字段定义块: %s", columns_block[:200] if len(columns_block) > 200 else columns_block)
    
    fields = []
    
    # 分割字段定义，需要考虑：
    # 1. 括号内的逗号（如 DECIMAL(10,2)）
    # 2. 引号内的逗号（如 COMMENT '应用类型:1 IOS,0 Android'）
    # 使用状态机来跟踪括号和引号的嵌套
    depth = 0  # 括号深度
    in_single_quote = False  # 是否在单引号内
    in_double_quote = False  # 是否在双引号内
    current_field = ""
    
    i = 0
    while i < len(columns_block):
        char = columns_block[i]
        
        # 处理转义字符（如 \' 或 \"）
        if char == '\\' and i + 1 < len(columns_block):
            current_field += char + columns_block[i + 1]
            i += 2
            continue
        
        if char == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
            current_field += char
        elif char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            current_field += char
        elif char == '(' and not in_single_quote and not in_double_quote:
            depth += 1
            current_field += char
        elif char == ')' and not in_single_quote and not in_double_quote:
            depth -= 1
            current_field += char
        elif char == ',' and depth == 0 and not in_single_quote and not in_double_quote:
            # 遇到顶层逗号（不在括号、单引号、双引号内），处理当前字段
            if current_field.strip():
                field_info = _parse_single_field(current_field.strip())
                if field_info:
                    fields.append(field_info)
            current_field = ""
        else:
            current_field += char
        
        i += 1
    
    # 处理最后一个字段
    if current_field.strip():
        field_info = _parse_single_field(current_field.strip())
        if field_info:
            fields.append(field_info)
    
    logger.info("从建表语句中解析出 %d 个字段", len(fields))
    return fields


def _parse_single_field(field_def: str) -> Dict[str, str]:
    """
    解析单个字段定义。
    
    字段定义格式示例：
    - `id` INT(11) NOT NULL AUTO_INCREMENT COMMENT '主键ID'
    - `name` VARCHAR(255) COMMENT '名称'
    - `amount` DECIMAL(10,2) DEFAULT 0 COMMENT '金额'
    
    会忽略约束定义，如：
    - PRIMARY KEY (`id`)
    - KEY `idx_name` (`name`)
    - UNIQUE KEY `uk_email` (`email`)
    - FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)
    
    Returns:
        字段信息字典，包含：字段名、字段数据类型、字段注释；如果是约束定义则返回 None
    """
    field_def = field_def.strip()
    if not field_def:
        return None
    
    # 检测是否是约束定义（PRIMARY KEY、KEY、UNIQUE KEY、FOREIGN KEY、INDEX 等）
    constraint_patterns = [
        r'^\s*PRIMARY\s+KEY',
        r'^\s*UNIQUE\s+KEY',
        r'^\s*UNIQUE\s+INDEX',
        r'^\s*FOREIGN\s+KEY',
        r'^\s*KEY\s+',
        r'^\s*INDEX\s+',
        r'^\s*CONSTRAINT\s+',
    ]
    
    for pattern in constraint_patterns:
        if re.match(pattern, field_def, re.IGNORECASE):
            logger.debug("跳过约束定义: %s", field_def[:50])
            return None
    
    # 移除反引号（如果存在）
    field_def_clean = field_def.replace('`', '')
    
    # 提取字段名（第一个单词，可能被反引号包围）
    field_name_match = re.match(r'^(\w+)', field_def_clean)
    if not field_name_match:
        logger.warning("无法解析字段名: %s", field_def[:50])
        return None
    
    field_name = field_name_match.group(1)
    
    # 检查字段名是否是约束关键字（如 PRIMARY、KEY 等）
    constraint_keywords = ['PRIMARY', 'KEY', 'UNIQUE', 'FOREIGN', 'INDEX', 'CONSTRAINT']
    if field_name.upper() in constraint_keywords:
        logger.debug("跳过约束关键字行: %s", field_def[:50])
        return None
    
    # 移除字段名部分
    remaining = field_def_clean[len(field_name):].strip()
    
    # 提取数据类型（可能包含括号，如 INT(11), DECIMAL(10,2)）
    # 匹配：类型名(可选参数)
    type_match = re.match(r'^(\w+(?:\([^)]+\))?)', remaining, re.IGNORECASE)
    if not type_match:
        logger.warning("无法解析字段类型: %s", remaining[:50])
        return None
    
    field_type = type_match.group(1)
    
    # 检查类型是否是约束关键字
    if field_type.upper() in constraint_keywords:
        logger.debug("跳过约束类型行: %s", field_def[:50])
        return None
    
    # 移除类型部分
    remaining = remaining[len(field_type):].strip()
    
    # 提取注释（COMMENT '...' 或 COMMENT "...")
    # 需要确保只匹配 COMMENT 关键字后面的引号，避免匹配到 DEFAULT '' 中的单引号
    comment = ""
    # 查找 COMMENT 关键字的位置
    comment_pos = re.search(r'\bCOMMENT\s+', remaining, re.IGNORECASE)
    if comment_pos:
        comment_part = remaining[comment_pos.end():].strip()
        # 匹配 COMMENT 后面的单引号内容（非贪婪匹配）
        comment_match = re.match(r"'(.*?)'", comment_part)
        if comment_match:
            comment = comment_match.group(1)
        else:
            # 尝试匹配双引号
            comment_match = re.match(r'"(.*?)"', comment_part)
            if comment_match:
                comment = comment_match.group(1)
    
    # 优化5：规范化字段注释，将特殊字符转换为单个空格（无论是否找到注释都进行规范化）
    comment = normalize_field_comment(comment)
    
    result = {
        "字段名": field_name,
        "字段数据类型": field_type,
        "字段注释": comment
    }
    
    logger.debug("解析字段: %s", result)
    return result

