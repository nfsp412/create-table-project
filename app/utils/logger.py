"""日志工具模块"""
import logging
import sys
from pathlib import Path

# 将项目根目录添加到Python路径
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from app.config.settings import LOG_DATE_FORMAT, LOG_DIR, LOG_FORMAT


def setup_logging(debug: bool = False, log_dir: str | Path | None = None) -> None:
    """
    配置统一的日志格式：
    2026-01-05 12:00:00 | INFO     | create_table | 消息内容
    当 log_dir 有值时，日志文件写入 log_dir/create_table.log；否则使用项目内 LOG_DIR。
    """
    file_log_dir = Path(log_dir) if log_dir else Path(LOG_DIR)
    file_log_dir.mkdir(parents=True, exist_ok=True)
    log_file = file_log_dir / "create_table.log"

    log_level = logging.DEBUG if debug else logging.INFO

    logging.basicConfig(
        level=log_level,
        format=LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(str(log_file), encoding="utf-8"),
        ],
    )
