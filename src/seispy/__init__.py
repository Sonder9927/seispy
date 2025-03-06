from seispy import collate, response
from seispy.download import IRISDownloader
from seispy.resample import resample_last_subdirs


def hello() -> str:
    return "Hello from seispy!"


__all__ = ["hello", "response", "resample_last_subdirs", "collate", "IRISDownloader"]
