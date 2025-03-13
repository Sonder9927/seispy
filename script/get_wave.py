import logging
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from obspy import UTCDateTime
from obspy.clients.fdsn import Client
from obspy.clients.fdsn.header import FDSNNoDataException
from tqdm import tqdm

LOG_FILE = "download.log"
NET = "net"
client = Client("IRIS", user="<email>", password="<token>")


def download(sta_list, start, end):
    dates = list(date_generator(start, end))
    tasks_num = len(sta_list) * len(dates)
    logging.info(f"Download start, total {tasks_num} tasks.")
    postfix = {"success": 0, "failed": 0, "skipped": 0, "nodata": 0}
    with ProcessPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(download_worker, sta, date)
            for sta in sta_list
            for date in dates
        }
        with tqdm(total=tasks_num, desc="Downloading: ") as pbar:
            for future in as_completed(futures):
                result = future.result()
                postfix[result] += 1
                pbar.update(1)
                pbar.set_postfix(postfix)
    summary = (
        "Download complete.\n"
        f"Success: {postfix['success']}, Skipped: {postfix['skipped']}, "
        f"NoData: {postfix['nodata']}, Failed: {postfix['failed']}."
    )
    logging.info(summary)
    print(summary)
    print(f'Check "{LOG_FILE}" for details.')


def download_worker(sta, date):
    year = str(date.year)
    julday = f"{date.julday:03d}"
    dest_dir = Path(NET) / sta / year / julday
    if dest_dir.exists() and len(list(dest_dir.iterdir())) == 3:
        return "skipped"
    try:
        st = client.get_waveforms(
            network=NET,
            station=sta,
            starttime=date,
            endtime=date + 86400,
            location="*",
            channel="*",
        )
        dest_dir.mkdir(parents=True, exist_ok=True)
        for tr in st:
            cha = tr.stats.channel
            fname = f"{NET}.{sta}.{year}.{julday}.{cha}.sac"
            tr.write(str(dest_dir / fname), format="SAC")
        return "success"
    except FDSNNoDataException:
        logging.warning(f"No Data for {sta} in {date}.")
        return "nodata"
    except Exception as e:
        logging.error(f"error date: {date} {e}.")
        return "failed"


def date_generator(start, end):
    current = start
    while current < end:
        yield current
        current += 86400


def remove_by_size(limit: int):
    size_limit = limit * 1024 * 1024
    size_max = 34_560_636
    for ifile in Path(NET).rglob("*.sac"):
        file_size = ifile.stat().st_size
        if file_size < size_limit:
            ifile.unlink()


if __name__ == "__main__":
    sta_list = [i.name for i in Path(NET).glob("*")]
    logging.basicConfig(
        level=logging.ERROR,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(LOG_FILE)],
    )
    num = 10
    while num > 0:
        remove_by_size(10)
        download(sta_list, UTCDateTime(2023, 1, 1), UTCDateTime(2025, 1, 1))
        time.sleep(1)
        num -= 1
