"""项目配置文件"""
import os
from pathlib import Path

# Excel文件路径
EXCEL_PATH = "/Users/sunpeng9/Documents/cursor-project/demo/create_table_info.xlsx"

# 输出目录
OUTPUT_DIR = "/Users/sunpeng9/Documents/cursor-project/demo"

# 日志配置
# 项目根目录：app/config/settings.py -> 向上3级到项目根目录
PROJECT_ROOT = Path(__file__).parent.parent.parent
LOG_DIR = str(PROJECT_ROOT / "logs")
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

