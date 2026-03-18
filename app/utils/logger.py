"""日志工具模块"""
import logging
import os
import sys
from pathlib import Path

# 将项目根目录添加到Python路径
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from app.config.settings import LOG_DIR, LOG_DATE_FORMAT, LOG_FORMAT


def setup_logging(debug: bool = False) -> None:
    """
    配置统一的日志格式：
    2026-01-05 12:00:00 | INFO     | create_table | 消息内容
    """
    # 确保日志目录存在
    Path(LOG_DIR).mkdir(parents=True, exist_ok=True)
    
    # 设置日志级别
    log_level = logging.DEBUG if debug else logging.INFO
    
    # 配置日志格式
    logging.basicConfig(
        level=log_level,
        format=LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT,
        handlers=[
            logging.StreamHandler(),  # 控制台输出
            logging.FileHandler(
                os.path.join(LOG_DIR, "create_table.log"),
                encoding="utf-8"
            ),  # 文件输出
        ],
    )
