# from seispy.download import IRISDownloader
from seispy import collate, event, response
from seispy.resample import resample_by_station, resample_to
from seispy.correct.clock_drift import correct_clock_drift


def hello() -> str:
    return "Hello from SeisPy!"


__all__ = [
    "hello",
    "response",
    "resample_by_station",
    "resample_to",
    "collate",
    "event",
    "correct_clock_drift"
]
