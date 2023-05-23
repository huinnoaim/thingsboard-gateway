from __future__ import annotations
import os
import logging
import time
import json
import threading
import multiprocessing as mp
import asyncio
import datetime as dt
from typing import Any, Union
from pathlib import Path
from traceback import print_exc
from zoneinfo import ZoneInfo

import aiohttp
import yaml

from redis_client import Redis, RedisUtils
from mqtt_client import MQTTClient
from datamodel import ECGBulk, ECG, HeartRate, HeartRateTelemetry
import hr_detector


logger = logging.getLogger(__file__)

DEFAULT_CFG_PATH = '/config/client.yaml'


class ECGWatcher(mp.Process):
    def __init__(self, ecg_queue: mp.Queue, ai_queue: mp.Queue, cfg_fpath: Union[Path, None] = None):
        super().__init__()
        self.name = 'ECG Watcher'
        self.redis = None
        self.ecg_queue: mp.Queue[ECGBulk] = ecg_queue
        self.ai_queue: mp.Queue[ECGBulk] = ai_queue
        self.cfgpath = cfg_fpath
        self.last_ecg_idx = {}
        self.num_of_required_ecg = 24  # ecg interval is 2.5 sec, 2.5* 24 = 60 sec
        self.redis_retry_sec = 5

    def run(self):
        self.redis = Redis.from_cfgfile(self.cfgpath)
        while not self.redis.is_connected:
            try:
                self.redis = Redis.from_cfgfile(self.cfgpath)
                if self.redis.is_connected:
                    logger.info(f'Redis Client is connected: {self.redis.is_connected}')
                    break
                time.sleep(self.redis_retry_sec)
                logger.info(f"Fail to connect to the Redis Host. Retry after {self.redis_retry_sec} sec")
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
                    latests = RedisUtils.ECG.get_lastest_index(self.redis, device, self.num_of_required_ecg)
                    if not latests:  # No data
                        continue
                    if (device in self.last_ecg_idx) and \
                    (self.last_ecg_idx[device] == latests[0]):  # Data unchanged
                        continue
                    updated_ecgs[device] = latests

                # update the last index
                for device, latests in updated_ecgs.items():
                    self.last_ecg_idx[device] = latests[0]

                # load ECGs for calcuation a heart rate
                for device, latests in updated_ecgs.items():
                    if len(latests) != self.num_of_required_ecg:
                        continue  # skip until the device has enough ECGs to calculate a heart rate

                    # sort a Redis key by acending order to follow the ECG data order
                    sortby_index = lambda x: int(x.split(':')[2])
                    keys = sorted(latests, key=sortby_index)  # latest ECG is the last

                    # load ECGs for the Redis
                    ecgs: list[ECG] = []
                    for raw in map(json.loads, self.redis.mget(keys)):
                        ecg = ECG(device, raw['ts'], raw['ecg'])
                        ecgs.append(ecg)

                    self.ecg_queue.put(ECGBulk(ecgs))
                    self.ai_queue.put(ECGBulk(ecgs))
                    logger.info(f'Device {device} ECG data is transfered to HR Calculator, ECG Queue Size: {self.ecg_queue.qsize()}')
                    continue
            except KeyboardInterrupt:
                self.redis.quit()

            time.sleep(.2)


class HeartRateCalculator(threading.Thread):
    '''It calculates Heart Rates by using a multi-processing and put it to the outgoing queue.
    '''
    def __init__(self, incoming_queue: mp.Queue, outgoing_queue: mp.Queue):
        super().__init__()
        self.setName('Heart Rate Calculator')
        self.ecg_queue: mp.Queue[ECGBulk] = incoming_queue
        self.hr_queue: mp.Queue[HeartRate] = outgoing_queue
        self.itersize = 40
        self.num_jobs = 10

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
            with mp.Pool(processes=num_of_processes) as pool:
                heart_rates = pool.map(HeartRateCalculator.calculate_hr, ecgbulks)
            for heart_rate in heart_rates:
                self.hr_queue.put(heart_rate)
                logger.debug(f"{heart_rate}")
            logger.info(f"HRs are transfered to HR Sender, HR Queue Size: {self.hr_queue.qsize()}")
            time.sleep(.2)

    @staticmethod
    def calculate_hr(ecgbulk: ECGBulk) -> HeartRate:
        hr = hr_detector.detect(ecgbulk.values, 250)
        milliseconds = round(time.time() * 1000)
        return HeartRate(ecgbulk.device, milliseconds, hr)


class HeartRateSender(mp.Process):
    '''It sends heart rates to the MQTT hosts.
    '''
    def __init__(self, queue: mp.Queue, cfg_fpath: Union[Path, None] = None):
        super().__init__()
        self.name = 'Heart Rate Sender'
        self.queue: mp.Queue[HeartRate] = queue
        self.tb_msg_queue: list[str] = []
        self.mq_msg_queue: list[str] = []
        self.cfg_path = cfg_fpath
        self.itersize = 40

    def run(self):
        self.tb_client = MQTTClient.from_cfgfile('thingsboard', self.cfg_path)
        self.tb_client.start()
        self.mq_client = MQTTClient.from_cfgfile('mosquitto', self.cfg_path)
        self.mq_client.start()

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
                time.sleep(.2)
            except KeyboardInterrupt:
                self.tb_client.disconnect()


    def handle_thingsboard(self, hr_telemetry: HeartRateTelemetry):
        msg = hr_telemetry.export_thingsboard_message()
        if msg is None:
            return

        # if disconnected to Thingbosard, queuing the HR message
        self.tb_client.pub(topic=hr_telemetry.THINGSBOARD_TOPIC, message=msg)
        if not self.tb_client.is_connected:
            self.tb_msg_queue.append(msg)
            if len(self.tb_msg_queue) % 1000 == 0:
                logger.warning(f"Thingsboard is disconnected, #{len(self.tb_msg_queue)} HRs are queued")
            return

        # if HR messages are queued and mqtt client is connected, publish the messages
        if self.tb_msg_queue and self.tb_client.is_connected:
            for msg in self.tb_msg_queue:
                self.tb_client.pub(topic=hr_telemetry.THINGSBOARD_TOPIC, message=msg)
            logger.info(f"Thignsboard is connected, #{len(self.tb_msg_queue)} HRs are sent")
            self.tb_msg_queue = []

    def handle_mosquitto(self, hr_telemetry: HeartRateTelemetry):
        msgs = hr_telemetry.export_mosquitto_messages()
        if msgs is None:
            return
        for msg in msgs:
            self.mq_client.pub(topic=hr_telemetry.MOSQUITTO_TOPIC, message=msg)

        # if disconnected to Mosquitto, queuing the HR message
        if not self.mq_client.is_connected:
            self.mq_msg_queue.append(msgs)
            if len(self.mq_msg_queue) % 1000 == 0:
                logger.warning(f"Mosquitto is disconnected, #{len(self.mq_msg_queue)} HRs are queued")
            return

        # if HR messages are queued and mqtt client is connected, publish the messages
        if self.mq_msg_queue and self.mq_client.is_connected:
            for msgs in self.mq_msg_queue:
                for msg in msgs:
                    self.mq_client.pub(topic=hr_telemetry.MOSQUITTO_TOPIC, message=msg)
            logger.info(f"Mosquitto is connected, #{len(self.tb_msg_queue)} HRs are sent")
            self.mq_msg_queue = []


class ECGUploader(mp.Process):
    '''It uploads ECGs to the AI Server.
    '''
    def __init__(self, queue: mp.Queue, host: str, access_token: str, upload_period: int):
        super().__init__()
        self.queue: mp.Queue[ECGBulk] = queue
        self.host: str = host
        self.access_token: str = access_token
        self.upload_period: int = upload_period
        self.uploaded_status: dict[str, int] = {}
        self.itersize: int = 40

    def run(self):
        while True:
            # collect Device's ECGs data
            ecgbulks: list[ECGBulk] = []
            if not self.queue.empty():
                for _ in range(min(self.itersize, self.queue.qsize())):
                    ecgbulk = self.queue.get(True, 100)
                    if ecgbulk.device not in self.uploaded_status:
                        self.uploaded_status[ecgbulk.device] = 0
                    is_passed_oneminute = (self.uploaded_status[ecgbulk.device] + self.upload_period) > int(time.time())
                    if is_passed_oneminute:
                        continue
                    ecgbulks.append(ecgbulk)

            if len(ecgbulks) == 0:
                continue

            logger.info(f'#{len(ecgbulks)} ECGs will be sent to AI Server')
            asyncio.run(self.upload_ecg_tasks(ecgbulks))
            # self.cancel_pending_tasks()
            logger.info(f'{self.upload_time}')
            time.sleep(5)

    async def upload_ecg(self, ecgbulk: ECGBulk) -> aiohttp.ClientResponse:
        body = {
            "serialNumber": ecgbulk.device,
            "requestTimestamp": int(time.time()),
            "requestSeconds": 60,
            "ecgData": ecgbulk.values
        }
        payload = json.dumps(body)
        headers = {
            'Content-Type': 'application/json',
            'iomt-jwt': self.access_token
        }
        self.uploaded_status[ecgbulk.device] = int(time.time())
        logger.info(f"Device {ecgbulk.device}'s ECGs will be sent to AI Server")
        async with aiohttp.ClientSession() as session:
            async with session.post(self.host, headers=headers, data=payload) as response:
                return response

    async def upload_ecg_tasks(self, ecgbulks: list[ECGBulk]):
        tasks = [asyncio.create_task(self.upload_ecg(ecgbulk)) for ecgbulk in ecgbulks]
        await asyncio.gather(*tasks)

    def cancel_pending_tasks(self):
        pending = asyncio.all_tasks()
        logger.info(f'#{len(pending)} Requests are in pending. They will be canceled')
        for task in pending:
            task.cancel()

        try:
            asyncio.run(asyncio.gather(*pending, return_exceptions=True))
        except asyncio.CancelledError:
            pass

    @property
    def upload_time(self, timezone: str = 'Asia/Seoul') -> dict[str, str]:
        status = {}
        tz = ZoneInfo(timezone)
        for device, epoch in self.uploaded_status.items():
            datetime = dt.datetime.fromtimestamp(epoch)
            status[device] = str(datetime.astimezone(tz))
        return status

    @staticmethod
    def from_cfgfile(queue: mp.Queue, fpath: Path) -> ECGUploader:
        dirname = os.path.dirname(os.path.abspath(__file__))
        cfg_file = dirname + DEFAULT_CFG_PATH.replace('/', os.path.sep)
        cfg_file = cfg_file if fpath is None else fpath

        with open(cfg_file) as general_config:
            full_cfg = yaml.safe_load(general_config)

        cfg = full_cfg['aiServer']
        host = cfg['host']
        access_token = cfg['accessToken']
        upload_period = cfg['uploadPeriodSec']
        return ECGUploader(queue, host, access_token, upload_period)
