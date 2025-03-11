# from seispy.download import IRISDownloader
from seispy import collate, event, response
from seispy.resample import resample_by_station


def hello() -> str:
    return "Hello from seispy!"


__all__ = ["hello", "response", "resample_by_station", "collate", "event"]
