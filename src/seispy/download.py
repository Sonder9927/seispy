import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from obspy import UTCDateTime
from obspy.clients.fdsn import Client
from tqdm import tqdm


class IRISDownloader:
    def __init__(self, config):
        """
        初始化下载器

        :param config: 字典配置，需包含：
            - email: IRIS账号邮箱
            - token: IRIS访问令牌
            - network: 台网代码 (如1U)
            - start_date: 起始日期 (格式: "YYYY-MM-DD")
            - end_date: 结束日期 (格式: "YYYY-MM-DD" 或 "now"表示当前日期)
        """
        self.config = {
            "station": "*",
            "location": "*",
            "channel": "*",
            "max_workers": 5,  # 建议不超过6
            "request_interval": 0.2,  # 请求间隔（秒），控制QPS≈0.83
            **config,  # 用户配置覆盖默认
        }
        self.client = Client(
            "IRIS", user=self.config["email"], password=self.config["token"]
        )
        self.executor = ThreadPoolExecutor(max_workers=self.config["max_workers"])
        # 日期有效性校验
        self._validate_dates()

    def _validate_dates(self):
        """验证日期范围合法性"""
        self.start_date = UTCDateTime(self.config["start_date"])
        end_date = self.config["end_date"]
        self.end_date = (
            UTCDateTime.now() - 86400
            if end_date.lower() == "now"
            else UTCDateTime(end_date)
        )

        if self.start_date >= self.end_date:
            logging.error("起始日期必须早于结束日期")
            raise

    def _safe_date_iter(self):
        """生成日期并检查进度"""
        current = self.start_date
        while current < self.end_date:
            yield current.datetime
            current += 86400

    def _build_filename(self, stats):
        """构建SAC文件名"""
        loc_code = stats.location.ljust(2, "-")[:2]
        return (
            f"{stats.network}.{stats.station}.{loc_code}."
            f"{stats.channel}.{stats.mseed.dataquality}."
            f"{stats.starttime.year}.{stats.starttime.julday:03d}"
            f"{stats.starttime.strftime('%H%M%S')}.sac"
        )

    def _process_trace(self, trace, out_dir: Path):
        """处理单个Trace"""
        stats = trace.stats
        try:
            # 构建目录路径
            dir_path = (
                out_dir
                / stats.network
                / stats.station
                / str(stats.starttime.year)
                / f"{stats.starttime.julday:03d}"
            )
            dir_path.mkdir(parents=True, exist_ok=True)

            # 保存文件
            filename = self._build_filename(stats)
            trace.write(str(dir_path / filename), format="SAC")
            return True
        except Exception as e:
            logging.error(f"Trace 保存失败 {stats.id}: {str(e)}")
            return False

    def _download_day(self, day, out_dir: Path):
        """下载单日数据"""
        day_str = day.strftime("%Y-%m-%d")
        try:
            # 获取数据
            st = self.client.get_waveforms(
                network=self.config["network"],
                station=self.config["station"],
                location=self.config["location"],
                channel=self.config["channel"],
                starttime=UTCDateTime(day),
                endtime=UTCDateTime(day) + 86400 - 0.001,
            )

            # 并行处理Trace
            futures = [
                self.executor.submit(self._process_trace, tr, out_dir) for tr in st
            ]
            results = [f.result() for f in as_completed(futures)]

            success = sum(results)
            logging.info(f"{day_str} finish: {success}/{len(results)} success")
            return True
        except Exception as e:
            logging.error(f"{day_str} error: {str(e)}")
            return False

    def run(self, output_dir: str):
        """run download"""
        dates = list(self._safe_date_iter())
        out_dir = Path(output_dir)

        with tqdm(total=len(dates), desc="Download progress") as pbar:
            futures = {
                self.executor.submit(self._download_day, day, out_dir): day
                for day in dates
            }
            for future in as_completed(futures):
                pbar.update(1)
                pbar.set_postfix(current=futures[future].strftime("%Y-%m-%d"))


if __name__ == "__main__":
    # 日志配置
    logging.basicConfig(
        filename="download.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        filemode="w",  # 每次运行新建日志
    )
    config = {
        "email": "your_email@example.com",
        "token": "your_token",
        "network": "1U",
        "start_date": "2024-08-01",
        "end_date": "2024-08-08",
        "channel": "BHZ",
    }

    downloader = IRISDownloader(config)
    downloader.run(output_dir="nz_1u/")
    print("任务完成！使用命令 'tail -f download.log' 查看实时日志")
