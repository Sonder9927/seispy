import logging
from pathlib import Path
from typing import Optional

from icecream import ic


def logger(
    name: str, log_file: Optional[Path] = None, level: int = logging.INFO
) -> logging.Logger:
    """为单个模块创建独立日志配置

    Args:
        name: 模块唯一标识 (如 'log_name')
        log_file: 日志文件路径 (None表示不写文件)
        level: 日志级别
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # 避免重复添加处理器
    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )

        if log_file:
            log_file.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

    return logger


def write_errors(errs, errs_txt="errors.txt"):
    with open(errs_txt, "w") as f:
        for err in errs:
            f.write(err)
    ic(f"Check {errs_txt} for more information")
