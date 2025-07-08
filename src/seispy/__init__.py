# from seispy.download import IRISDownloader
from seispy import collate, event, response
from seispy.resample import resample_by_station, resample_to


def hello() -> str:
    return "Hello from SeisPy!"


__all__ = [
    "hello",
    "response",
    "resample_by_station",
    "resample_to",
    "collate",
    "event",
]
