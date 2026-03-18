"""项目配置文件"""
from pathlib import Path

# 项目根目录：app/config/settings.py -> 向上3级到项目根目录
PROJECT_ROOT = Path(__file__).parent.parent.parent

# Excel 文件名（实际路径在 main.py 中按运行日期拼接）
EXCEL_FILENAME = "create_table_info.xlsx"

# 输出根目录（实际输出时会在 main.py 中追加日期子目录）
OUTPUT_BASE_DIR = PROJECT_ROOT.parent / "create-table-output"

# 日志配置
LOG_DIR = str(PROJECT_ROOT / "logs")
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
