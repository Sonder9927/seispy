import logging
from pathlib import Path

from concurrent_log_handler import ConcurrentRotatingFileHandler
from icecream import ic


def get_logger(name: str, file=None, level=logging.INFO) -> logging.Logger:
    """获取或创建指定名称的logger，确保仅配置一次。

    Args:
        name: 模块唯一标识 (如 'log_name')
        log_file: 日志文件路径 (None表示不写文件)
        level: 日志级别

    Returns:
        logging.Logger: 日志记录器
    """
    logger = logging.getLogger(name)

    # 避免重复添加处理器
    if not logger.handlers:
        logger.setLevel(level)
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

        if file:
            file_dir = Path("logs")
            file_dir.mkdir(parents=True, exist_ok=True)
            file_handler = ConcurrentRotatingFileHandler(
                file_dir / file, mode="a", maxBytes=1024 * 1024 * 1, backupCount=5
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

    return logger


def write_errors(errs, errs_txt="errors.txt"):
    with open(errs_txt, "w") as f:
        for err in errs:
            f.write(err)
    ic(f"Check {errs_txt} for more information")
