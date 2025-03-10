import logging
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Tuple, Union

from obspy import Stream, UTCDateTime
from obspy.clients.fdsn import Client
from obspy.clients.fdsn.header import FDSNNoDataException
from tqdm import tqdm

LOG_FILE_DOWNLOAD = "download.log"


class IRISDownloader:
    def __init__(self, config: dict):
        """
        初始化下载器

        :param config: 字典配置，需包含：
            - email: IRIS账号邮箱
            - token: IRIS访问令牌
            - network: 台网代码 (如1U)
            - start_date: 起始日期 (格式: "YYYY-MM-DD")
            - end_date: 结束日期 (格式: "YYYY-MM-DD" 或 "now"表示当前日期)
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config = {
            "station": "*",
            "location": "*",
            "channel": "*",
            "max_workers": 5,  # 建议不超过6
            "request_interval": 0.2,  # 请求间隔（秒），控制QPS≈0.83
            **config,  # 用户配置覆盖默认
        }
        self.connection_params = {
            "base_url": "IRIS",
            "user": self.config["email"],
            "password": self.config["token"],
        }
        # self.client = Client(
        #     "IRIS",
        #     user=self.config["email"],
        #     password=self.config["token"],
        #     # user_agent=f"SUSTech_SeismoLab/1.0 ({self.config['email']})",
        # )

        self._validate_dates()
        # self._init_stations()
        self.executor = ProcessPoolExecutor(max_workers=self.config["max_workers"])

    def _validate_dates(self):
        """验证并初始化日期范围"""
        self.start_date = UTCDateTime(self.config["start_date"])
        end_date = self.config["end_date"]
        if end_date.lower() == "now":
            self.end_date = UTCDateTime.now() - 86400  # 避免获取不完整的天数据
        else:
            self.end_date = UTCDateTime(end_date)

        if self.start_date >= self.end_date:
            error_msg = "起始日期必须早于结束日期"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

    def _get_stations(self) -> List[str]:
        """获取台站列表（如果用户未提供）"""
        self.logger.info("Fetching station list...")
        client = self._create_client(self.connection_params)

        try:
            inventory = client.get_stations(
                network=self.config["network"],
                station=self.config["station"],
                starttime=self.start_date,
                endtime=self.end_date,
                level="station",
            )
            return list({sta.code for net in inventory for sta in net.stations})
        except Exception as e:
            self.logger.error(f"Failed to get stations: {str(e)}")
            raise

    def _dates(self) -> List[UTCDateTime]:
        """生成日期范围列表"""
        dates = []
        current = self.start_date
        while current < self.end_date:
            dates.append(current)
            current += 86400
        return dates

    def _build_filename(self, stats):
        """构建SAC文件名"""
        # loc_code = stats.location.ljust(2, "-")[:2]
        return (
            f"{stats.network}.{stats.station}.{stats.channel}."
            f"{stats.starttime.year}.{stats.starttime.julday:03d}.sac"
        )

    def _process_single_station_day(
        self, client: Client, day: UTCDateTime, station: str, base_path: Path
    ) -> bool:
        """处理单个台站单日数据下载"""
        day_str = day.strftime("%Y-%m-%d")
        self.logger.debug(f"Processing: {station}/{day_str}")
        save_path = (
            base_path
            / self.config["network"]
            / station
            / str(day.year)
            / f"{day.julday:03d}"
        )

        # 跳过已存在数据的目录
        if save_path.exists() and any(save_path.iterdir()):
            self.logger.info(f"Exists: {station}/{day_str}")
            return True

        try:
            # 遵守API请求频率限制
            time.sleep(self.config["request_interval"])

            stream = self.client.get_waveforms(
                network=self.config["network"],
                station=station,
                location=self.config["location"],
                channel=self.config["channel"],
                starttime=day,
                endtime=day + 86400,
            )

            if not isinstance(stream, Stream) or len(stream) == 0:
                self.logger.warning(f"No Data: {station}/{day_str}")
                return False

            save_path.mkdir(parents=True, exist_ok=True)
            for trace in stream:
                filename = self._build_filename(trace.stats)
                trace.write(str(save_path / filename), format="SAC")

            self.logger.debug(f"Downloaded: {station}/{day_str}")
            return True

        except FDSNNoDataException:
            self.logger.warning(f"No Data: {station}/{day_str}")
            return False
        except Exception as e:
            self.logger.error(f"Download failed: {station}/{day_str}: {str(e)}")
            return False

    @staticmethod
    def _create_client(params: Dict) -> Client:
        """在子进程内创建客户端"""
        return Client(
            base_url=params["base_url"],
            user=params["user"],
            password=params["password"],
            user_agent=f"IRISDownloader/1.0 ({params['user']})",
            timeout=30,
        )

    @staticmethod
    def _process_task(args: Tuple) -> bool:
        """任务处理入口（静态方法避免序列化问题）"""
        params, config, day, station, base_path = args
        client = IRISDownloader._create_client(params)
        downloader = IRISDownloader(config)
        return downloader._process_single_station_day(client, day, station, base_path)

    def wave(self, output_dir: str):
        """启动下载任务"""
        base_path = Path(output_dir)
        dates = self._dates()
        stations = self.config["stations"]
        total_tasks = len(dates) * len(stations)

        self.logger.info(f"Download start, all {total_tasks} tasks.")

        with tqdm(total=total_tasks, desc="Download progress") as progress_bar:
            futures = [
                self.executor.submit(
                    self._process_single_station_day, day, station, base_path
                )
                for station in stations
                for day in dates
            ]

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Mission failed: {str(e)}")
                finally:
                    progress_bar.update(1)

        self.executor.shutdown()
        self.logger.info("Mission complete.")
        print(f"Mission complete. Check {LOG_FILE_DOWNLOAD} for details.")

    def response(self, outfile: str, **kwargs):
        self.logger.info("Download response start.")
        try:
            inventory = self._create_client().get_stations(
                network=self.config["network"], level="response", **kwargs
            )
            inventory.write(outfile, format="STATIONXML")
        except Exception as e:
            self.logger.error(f"Mission failed: {str(e)}")
        self.logger.info("Mission complete.")
        print(f"Mission complete. Check {LOG_FILE_DOWNLOAD} for details.")


if __name__ == "__main__":
    # 示例配置
    config = {
        "email": "your@email",
        "token": "your_token",
        "network": "1U",
        "start_date": "2024-01-01",
        "end_date": "2024-01-05",
        "channel": "BH*",  # 使用通配符下载多个通道
    }

    # 初始化日志配置（由调用方负责）
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("download.log"), logging.StreamHandler()],
    )

    downloader = IRISDownloader(config)
    downloader.wave("out_data/")
    print("任务完成！使用命令 'tail -f download.log' 查看实时日志")
