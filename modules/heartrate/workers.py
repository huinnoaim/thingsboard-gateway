from __future__ import annotations
import os
import logging
import time
import json
import threading
import multiprocessing as mp
import asyncio
import datetime as dt
from typing import Any, Union, NamedTuple
from pathlib import Path
from traceback import print_exc
from zoneinfo import ZoneInfo
import dataclasses as dc

import aiohttp
import yaml
import numpy as np

from connectors import Redis, RedisUtils, MQTTClient
from heartrate.datamodel import ECGBulk, ECG, HeartRate, HeartRateTelemetry
import heartrate.neurokit as nk


ONE_MINUTE_SEC = 60

logger = logging.getLogger(__name__)


class ECGPacketWatcher(mp.Process):
    def __init__(
        self,
        num_of_watching_packets: int,
        ecg_queue: mp.Queue,
        ai_queue: mp.Queue,
        queued_packet_cfg: ECGPacketWatcher.QueuedPacketConfig,
        cfg_fpath: Union[Path, None] = None,
    ):
        super().__init__()
        self.name = "ECG Watcher"
        self.redis = None
        self.ecg_queue: mp.Queue[ECGBulk] = ecg_queue
        self.ai_queue: mp.Queue[ECGBulk] = ai_queue
        self.cfgpath = cfg_fpath
        self.last_ecg_idx = {}
        self.num_of_watching_packets: int = num_of_watching_packets
        self.num_of_packets_for_ai: int = queued_packet_cfg.num_of_packets_for_ai
        self.num_of_packets_for_hr: int = queued_packet_cfg.num_of_packets_for_hr
        self.redis_retry_sec = 5

    def run(self):
        self.redis = Redis.from_cfgfile(self.cfgpath)
        while not self.redis.is_connected:
            try:
                self.redis = Redis.from_cfgfile(self.cfgpath)
                if self.redis.is_connected:
                    logger.info(f"Redis Client is connected: {self.redis.is_connected}")
                    break
                time.sleep(self.redis_retry_sec)
                logger.info(
                    f"Fail to connect to the Redis Host. Retry after {self.redis_retry_sec} sec"
                )
            except Exception as e:
                print_exc()
                logger.info(f"Redis Connection occurs an error: {e}")

        while True:
            RedisIdxes = list[str]
            DeviceName: str
            updated_ecgs: dict[DeviceName, RedisIdxes] = {}

            try:
                # find a device having a updated ECG data
                devices = RedisUtils.ECG.get_devices(self.redis)
                for device in devices:
                    latests = RedisUtils.ECG.get_lastest_index(
                        self.redis, device, self.num_of_watching_packets
                    )
                    if not latests:  # No data
                        continue
                    if (device in self.last_ecg_idx) and (
                        self.last_ecg_idx[device] == latests[0]
                    ):  # Data unchanged
                        continue
                    updated_ecgs[device] = latests

                # update the last index
                for device, latests in updated_ecgs.items():
                    self.last_ecg_idx[device] = latests[0]

                # load ECGs for calcuation a heart rate
                for device, latests in updated_ecgs.items():
                    if len(latests) != self.num_of_watching_packets:
                        continue  # skip until the device has enough ECGs to calculate a heart rate

                    # sort a Redis key by acending order to follow the ECG data order
                    sortby_index = lambda x: int(x.split(":")[2])
                    keys = sorted(latests, key=sortby_index)  # latest ECG is the last

                    # load ECGs for the Redis
                    ecgs: list[ECG] = []
                    try:
                        for raw in map(json.loads, self.redis.mget(keys)):
                            ecg = ECG(device, raw["ts"], raw["ecg"])
                            ecgs.append(ecg)
                    except TypeError as e:
                        print_exc()
                        logger.warning("Fail to read ECG data from Redis")

                    # No ECG data
                    if not ecgs:
                        continue

                    # transfer ECG data
                    self.ecg_queue.put(ECGBulk(ecgs[-self.num_of_packets_for_hr :]))
                    self.ai_queue.put(ECGBulk(ecgs[-self.num_of_packets_for_ai :]))
                    logger.info(
                        f"Device {device} ECG data is transfered to HR Calculator, ECG Queue Size: {self.ecg_queue.qsize()} / AI Queue Size: {self.ai_queue.qsize()}"
                    )
                    continue
            except KeyboardInterrupt:
                self.redis.quit()

            time.sleep(0.2)

    @staticmethod
    def from_cfgfile(
        ecg_queue: mp.Queue, ai_queue: mp.Queue, cfg_fpath: Path
    ) -> ECGPacketWatcher:
        with open(cfg_fpath) as general_config:
            full_cfg = yaml.safe_load(general_config)

        cfg = full_cfg["ecgPacketWatcher"]
        num_of_watching_packets = cfg["numOfWatchingPackets"]
        num_of_packets_for_ai = cfg["numOfPacketsForAiServer"]
        num_of_packets_for_hr = cfg["numOfPacketsForHeartRate"]

        return ECGPacketWatcher(
            num_of_watching_packets,
            ecg_queue,
            ai_queue,
            ECGPacketWatcher.QueuedPacketConfig(
                num_of_packets_for_ai=num_of_packets_for_ai,
                num_of_packets_for_hr=num_of_packets_for_hr,
            ),
            cfg_fpath,
        )

    class QueuedPacketConfig(NamedTuple):
        num_of_packets_for_ai: int
        num_of_packets_for_hr: int


class HeartRateCalculator(threading.Thread):
    """It calculates Heart Rates by using a multi-processing and put it to the outgoing queue."""

    def __init__(
        self,
        incoming_queue: mp.Queue,
        outgoing_queue: mp.Queue,
        packet_interval_time: float,
        window_size: int,
        n_jobs: int = 10,
    ):
        super().__init__()
        self.setName("Heart Rate Calculator")
        self.ecg_queue: mp.Queue[ECGBulk] = incoming_queue
        self.hr_queue: mp.Queue[HeartRate] = outgoing_queue
        self.total_packet_time = packet_interval_time * window_size
        self.num_jobs = n_jobs
        self.itersize = 40

    def run(self):
        while True:
            # collect ECGs
            ecgbulks: list[ECGBulk] = []
            if not self.ecg_queue.empty():
                for _ in range(min(self.itersize, self.ecg_queue.qsize())):
                    ecgbulk: ECGBulk = self.ecg_queue.get(True, 100)
                    ecgbulks.append(ecgbulk)
                logger.info(f"Queud ECGs: {self.ecg_queue.qsize()}")

            # assign a dynamic process by its queue
            num_of_processes = (len(ecgbulks) // self.num_jobs) + 1
            logger.info(f"#{num_of_processes} Processes calcuate #{len(ecgbulks)} HRs")

            # calculates Heart Rates and transfer it
            hr_inputs = zip(ecgbulks, [self.total_packet_time] * len(ecgbulks))
            with mp.Pool(processes=num_of_processes) as pool:
                heart_rates = pool.starmap(HeartRateCalculator.calculate_hr, hr_inputs)

            for heart_rate in filter(lambda x: x is not None, heart_rates):
                self.hr_queue.put(heart_rate)
                logger.debug(f"{heart_rate}")
            logger.info(
                f"HRs are transfered to HR Sender, HR Queue Size: {self.hr_queue.qsize()}"
            )
            time.sleep(1)

    def calculate_hr(ecgbulk: ECGBulk, total_packet_time: float) -> HeartRate | None:
        # hr: float = hr_detector.detect(ecgbulk.values, 250)
        try:
            rpeaks_idxes: np.ndarray[int] = nk.get_rpeaks(
                ecgbulk.values, 250
            )  # for 10 sec
        except IndexError as e:
            logger.debug(f"{ecgbulk.device} occurs index error")
            return None

        if rpeaks_idxes.size == 0:
            logger.debug(f"{ecgbulk.device} shows empty rpeaks: {rpeaks_idxes}")
            return None

        if np.diff(rpeaks_idxes).size == 0:
            logger.debug(f"{ecgbulk.device} shows empty rpeaks diffs: {rpeaks_idxes}")
            return None

        mean_rr_inerval = np.mean(np.diff(rpeaks_idxes))
        time_resolution = total_packet_time / len(ecgbulk.values)
        hr = ONE_MINUTE_SEC / (mean_rr_inerval * time_resolution)
        hr = round(hr)
        milliseconds = round(time.time() * 1000)
        return HeartRate(ecgbulk.device, milliseconds, hr)

    @staticmethod
    def from_cfgfile(
        ecg_queue: mp.Queue, hr_queue: mp.Queue, fpath: Path
    ) -> HeartRateCalculator:
        with open(fpath) as general_config:
            full_cfg = yaml.safe_load(general_config)

        cfg = full_cfg["heartRate"]["calculator"]
        num_jobs = cfg["jobsPerProcess"]
        packet_interval_time = cfg["packetIntervalTime"]
        window_size = cfg["windowSize"]
        return HeartRateCalculator(
            ecg_queue, hr_queue, packet_interval_time, window_size, num_jobs
        )


class HeartRateSender(mp.Process):
    """It sends heart rates to the MQTT hosts."""

    def __init__(self, queue: mp.Queue, cfg_fpath: Union[Path, None] = None):
        super().__init__()
        self.name = "Heart Rate Sender"
        self.queue: mp.Queue[HeartRate] = queue
        self.tb_msg_queue: list[str] = []
        self.mq_msg_queue: list[str] = []
        self.cfg_path = cfg_fpath
        self.itersize = 40

    def run(self):
        self.tb_client = MQTTClient.from_cfgfile("thingsboard", self.cfg_path)
        self.tb_client.start()
        self.mq_client = MQTTClient.from_cfgfile("mosquitto", self.cfg_path)
        self.mq_client.start()

        # For Alarm debugging
        try:
            self.mq_dev_client = MQTTClient.from_cfgfile("mosquitto-dev", self.cfg_path)
            self.mq_dev_client.start()
        except ValueError as e:
            self.mq_client.join()
            self.mq_dev_client = None
            logger.info("No DEV MQTT Broker")

        while True:
            try:
                # collect a heart rate
                hrs: list[HeartRate] = []
                if not self.queue.empty():
                    for _ in range(min(self.itersize, self.queue.qsize())):
                        hr = self.queue.get(True, 100)
                        hrs.append(hr)
                        logger.debug(f"{hr.device}'s HR is arrived to HR Sender")

                if not hrs:
                    continue

                # export a heart rate message and publish it to Thingsboard
                hr_telemetry = HeartRateTelemetry(hrs)
                self.handle_thingsboard(hr_telemetry)
                self.handle_mosquitto(hr_telemetry)
                time.sleep(0.2)
            except KeyboardInterrupt:
                self.tb_client.disconnect()

    def handle_thingsboard(self, hr_telemetry: HeartRateTelemetry):
        msg = hr_telemetry.export_thingsboard_message()
        if msg is None:
            logger.debug(f"MSG for Thingsboard is Null")
            return

        # if disconnected to Thingbosard, queuing the HR message
        self.tb_client.pub(topic=hr_telemetry.THINGSBOARD_TOPIC, message=msg)
        logger.debug(f"MSG to Thingsboard: {hr_telemetry.THINGSBOARD_TOPIC}/{msg}")
        if not self.tb_client.is_connected:
            self.tb_msg_queue.append(msg)
            if len(self.tb_msg_queue) % 1000 == 0:
                logger.warning(
                    f"Thingsboard is disconnected, #{len(self.tb_msg_queue)} HRs are queued"
                )
            return

        # if HR messages are queued and mqtt client is connected, publish the messages
        if self.tb_msg_queue and self.tb_client.is_connected:
            for msg in self.tb_msg_queue:
                self.tb_client.pub(topic=hr_telemetry.THINGSBOARD_TOPIC, message=msg)
            logger.info(
                f"Thignsboard is connected, #{len(self.tb_msg_queue)} HRs are sent"
            )
            self.tb_msg_queue = []

    def handle_mosquitto(self, hr_telemetry: HeartRateTelemetry):
        msgs = hr_telemetry.export_mosquitto_messages()
        if msgs is None:
            return
        for msg in msgs:
            self.mq_client.pub(topic=hr_telemetry.MOSQUITTO_TOPIC, message=msg)
            # for alarm debugging
            if self.mq_dev_client:
                self.mq_dev_client.pub(topic=hr_telemetry.MOSQUITTO_TOPIC, message=msg)

        # if disconnected to Mosquitto, queuing the HR message
        if not self.mq_client.is_connected:
            self.mq_msg_queue.append(msgs)
            if len(self.mq_msg_queue) % 1000 == 0:
                logger.warning(
                    f"Mosquitto is disconnected, #{len(self.mq_msg_queue)} HRs are queued"
                )
            return

        # if HR messages are queued and mqtt client is connected, publish the messages
        if self.mq_msg_queue and self.mq_client.is_connected:
            for msgs in self.mq_msg_queue:
                for msg in msgs:
                    self.mq_client.pub(topic=hr_telemetry.MOSQUITTO_TOPIC, message=msg)
            logger.info(
                f"Mosquitto is connected, #{len(self.tb_msg_queue)} HRs are sent"
            )
            self.mq_msg_queue = []


class ECGUploader(mp.Process):
    """It uploads ECGs to the AI Server."""

    def __init__(
        self, queue: mp.Queue, host: str, access_token: str, upload_period: int
    ):
        super().__init__()
        self.queue: mp.Queue[ECGBulk] = queue
        self.host: str = host
        self.access_token: str = access_token
        self.upload_period: int = upload_period
        self.uptodate_ecgs: dict[str, ECGUploader.ECG] = {}
        self.itersize: int = 40

    def run(self):
        logger.info(f"Start ECG Uplaoder")
        ecg_maintainer = threading.Thread(
            target=self.maintain_ecgs, name="ECG Maintainer"
        )
        ecg_maintainer.start()

        while True:
            upload_ecgs: dict[str, ECGUploader.ECG] = {}
            for device in self.uptodate_ecgs.keys():
                device_uptodate_ecg: ECGUploader.ECG = self.uptodate_ecgs[device]
                next_upload_ts = device_uptodate_ecg.uploaded_ts + self.upload_period
                if int(time.time()) < next_upload_ts:
                    continue

                upload_ecgs[device] = device_uptodate_ecg
                device_uptodate_ecg.uploaded_ts = int(time.time())
                logger.debug(
                    f"{device}{device_uptodate_ecg.ts_range} is added AI Server Upload Queue"
                )

            if upload_ecgs:
                logger.debug(f"#{len(upload_ecgs.keys())} Devices:{upload_ecgs.keys()}")
                asyncio.run(self.send_ai_server(upload_ecgs))
                logger.info(f"AI Server Upload Status: {self.upload_time}")
            time.sleep(5)

    def maintain_ecgs(self):
        """It keeps the ECG as the up-to-date values"""
        while True:
            if not self.queue.empty():
                for _ in range(min(self.itersize, self.queue.qsize())):
                    ecgbulk: ECGBulk = self.queue.get(True, 100)
                    if ecgbulk.device not in self.uptodate_ecgs:  # init ECG
                        self.uptodate_ecgs[ecgbulk.device] = ECGUploader.ECG()

                    # maintain the ECGs as up-to-date values
                    self.uptodate_ecgs[ecgbulk.device].values = ecgbulk.values
                    self.uptodate_ecgs[ecgbulk.device].ts_range = ecgbulk.ts_range
                time.sleep(1)

    async def send_ai_server(self, uptodate_ecgs: dict[str, ECGUploader.ECG]):
        tasks = []
        for device, ecg in uptodate_ecgs.items():
            task = asyncio.create_task(
                self.upload_ecg(device, ecg.values, ecg.start_ts, ecg.end_ts)
            )
            tasks.append(task)

        await asyncio.gather(*tasks)
        for task in tasks:
            logger.debug(f"AI Analysis Response: {task.result()}")

    async def upload_ecg(
        self, device: str, values: list[float], start_ts: int, end_ts: int
    ) -> str:
        body = {
            "serialNumber": device,
            "requestTimestamp": end_ts,
            "requestSeconds": 60,
            "startTimestamp": start_ts,
            "endTimestamp": end_ts,
            "ecgData": values,
        }
        payload = json.dumps(body)
        headers = {"Content-Type": "application/json", "iomt-jwt": self.access_token}
        logger.info(
            f"Device {device}'s #{len(values)} ECGs at {int(time.time())} will be sent to AI Server"
        )
        async with aiohttp.ClientSession() as session:
            async with session.post(
                self.host, headers=headers, data=payload
            ) as response:
                data = await response.read()
        return data.decode("utf-8")

    @property
    def upload_time(self, timezone: str = "Asia/Seoul") -> dict[str, str]:
        status = {}
        for device, ecg in self.uptodate_ecgs.items():
            status[device] = str(ecg.get_upload_dt(timezone))
        return status

    @staticmethod
    def from_cfgfile(queue: mp.Queue, fpath: Path) -> ECGUploader:
        with open(fpath) as general_config:
            full_cfg = yaml.safe_load(general_config)

        cfg = full_cfg["aiServer"]
        host = cfg["url"]
        access_token = cfg["accessToken"]
        upload_period = cfg["uploadPeriodSec"]
        return ECGUploader(queue, host, access_token, upload_period)

    @dc.dataclass
    class ECG:
        uploaded_ts: int = dc.field(default=0)
        ts_range: tuple[int] = dc.field(default=(0, 0))
        values: list[float] = dc.field(default_factory=list)

        @property
        def start_ts(self) -> int:
            return int(self.ts_range[0] / 1000)

        @property
        def end_ts(self) -> int:
            return int(self.ts_range[-1] / 1000)

        def get_upload_dt(self, timezone: str = "Asia/Seoul") -> dt.datetime:
            datetime = dt.datetime.fromtimestamp(self.uploaded_ts)
            return datetime.astimezone(ZoneInfo(timezone))
