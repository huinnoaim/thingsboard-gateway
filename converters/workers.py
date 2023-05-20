import logging
import time
import json
from queue import Queue
import multiprocessing as mp
from typing import Union
from pathlib import Path
from traceback import print_exc


from redis_client import Redis, RedisUtils
from mqtt_client import MQTTClient
from datamodel import ECGBulk, ECG, HeartRate, HeartRateTelemetry
import hr_detector


logger = logging.getLogger(__file__)

def fibonacci(n):
    if n <= 1:
        return n
    else:
        return fibonacci(n-1) + fibonacci(n-2)


class ECGWatcher(mp.Process):
    def __init__(self, queue: mp.Queue, cfg_fpath: Union[Path, None] = None):
        super().__init__()
        self.redis = None
        self.queue: mp.Queue[ECGBulk] = queue
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
            try:
                devices = RedisUtils.ECG.get_devices(self.redis)
                for device in devices:
                    latests = RedisUtils.ECG.get_lastest_index(self.redis, device, self.num_of_required_ecg)
                    if not latests:  # No data
                        continue

                    if (device in self.last_ecg_idx) and \
                    (self.last_ecg_idx[device] == latests[0]):  # Data unchanged
                        continue

                    self.last_ecg_idx[device] = latests[0]  # update the last index

                    if len(latests) == self.num_of_required_ecg:
                        sortby_index = lambda x: int(x.split(':')[2])
                        keys = sorted(latests, key=sortby_index)  # latest ECG is the last

                        ecgs: list[ECG] = []
                        for raw in map(json.loads, self.redis.mget(keys)):
                            ecg = ECG(device, raw['ts'], raw['ecg'])
                            ecgs.append(ecg)

                        self.queue.put(ECGBulk(ecgs))
                        logger.info(f'Device {device} ECG data is transfered')
                        # logger.info(f'Device {device} ECG data is transfered, ECG mp.Queue Size: {self.queue.qsize()}')
                        continue
            except KeyboardInterrupt:
                self.redis.quit()

            time.sleep(.2)


class HeartRateCalculator(mp.Process):
    '''It calculates Heart Rates by using a multi-processing and put it to the outgoing queue.
    '''
    def __init__(self, incoming_queue: mp.Queue, outgoing_queue: mp.Queue):
        super().__init__()
        self.ecg_queue: mp.Queue[ECGBulk] = incoming_queue
        self.hr_queue: mp.Queue[HeartRate] = outgoing_queue
        self.itersize = 40

    def run(self):
        while True:
            if not self.ecg_queue.empty():
                # for _ in range(min(self.itersize, self.ecg_queue.qsize())):
                ecgbulk: ECGBulk = self.ecg_queue.get(True, 100)
                heart_rate: HeartRate = HeartRateCalculator.calculate_hr(ecgbulk)
                self.hr_queue.put(heart_rate)
                logger.info(f"Device {ecgbulk.device} HR: {heart_rate}")
            # time.sleep(.2)

    @staticmethod
    def calculate_hr(ecgbulk: ECGBulk) -> HeartRate:
        hr = hr_detector.detect(ecgbulk.values, 250)
        milliseconds = round(time.time() * 1000)
        return HeartRate(ecgbulk.device, milliseconds, hr)


class HeartRateSender(mp.Process):
    TOPIC = "v1/gateway/telemetry"

    def __init__(self, queue: mp.Queue, cfg_fpath: Union[Path, None] = None):
        super().__init__()
        self.queue: mp.Queue[HeartRate] = queue
        self.msg_queue: list[str] = []
        self.cfgpath = cfg_fpath
        self.itersize = 40
        self.retry_sec = 5

    def run(self):
        self.mqtt_client = MQTTClient.from_cfgfile(self.cfgpath)
        while not self.mqtt_client.is_connected:
            try:
                logger.info(f"Try to connect to the MQTT Host: {self.mqtt_client.url}")
                self.mqtt_client.connect()
                if self.mqtt_client.is_connected:
                    logger.info(f'MQTT Client is connected: {self.mqtt_client.is_connected}')
                    break
                time.sleep(self.retry_sec)
                logger.info(f"Fail to connect to the MQTT Host. Retry after {self.retry_sec} sec")
            except Exception as e:
                print_exc()
                logger.info(f"MQTT Connection occurs an error: {e}")

        while True:
            try:
                if not self.queue.empty():
                    hrs: list[HeartRate] = []
                    hr = self.queue.get(True, 100)
                    logger.info(f"{hr.device}'s HR is arrived to HR Sender")
                    hrs.append(hr)
                    # hrs: list[HeartRate] = []
                    # for i in range(self.itersize):
                    #     print(i)
                    #     hr = self.queue.get(True, 100)
                    #     logger.info(f"{hr.device}'s HR is arrived to HR Sender")
                    #     print(len(hrs))

                    hr_telemetry = HeartRateTelemetry(hrs)
                    msg = hr_telemetry.export_message()
                    self.mqtt_client.pub(topic=self.TOPIC, message=msg)

                    if not self.mqtt_client.is_connected:
                        self.msg_queue.append(msg)
                        logger.warning(f"MQTT client is disconnected, #{len(self.msg_queue)} HRs are queued")
                        continue

                    if self.msg_queue and self.mqtt_client.is_connected:
                        for msg in self.msg_queue:
                            self.mqtt_client.pub(topic=self.TOPIC, message=msg)
                        logger.info(f"MQTT client is connected, #{len(self.msg_queue)} HRs are sent")
                        self.msg_queue = []

                time.sleep(.2)
            except KeyboardInterrupt:
                self.mqtt_client.disconnect()
