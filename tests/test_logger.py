import logging
import shutil
import tempfile
import unittest
from pathlib import Path

from app.utils.logger import setup_logging


class TestSetupLogging(unittest.TestCase):
    """setup_logging 日志输出路径测试。"""

    def setUp(self):
        self.tmp_dir = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_log_dir_writes_to_specified_directory(self):
        """当传入 log_dir 时，日志文件应写入该目录下的 create_table.log。"""
        setup_logging(debug=False, log_dir=self.tmp_dir)

        logger = logging.getLogger("test")
        logger.info("test message")

        log_file = self.tmp_dir / "create_table.log"
        self.assertTrue(log_file.exists())
        self.assertIn("test message", log_file.read_text(encoding="utf-8"))
