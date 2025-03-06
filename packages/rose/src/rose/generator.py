from typing import Generator


def batch_generator(items: list, batch_size: int) -> Generator[list, None, None]:
    """生成文件批次"""
    for i in range(0, len(items), batch_size):
        yield items[i: i + batch_size]
