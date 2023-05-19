import logging
import time
import json
import multiprocessing as mp
from typing import Union
from pathlib import Path
from multiprocessing import Queue
from traceback import print_exc

from redis_client import Redis, RedisUtils
from mqtt_client import MQTTClient
from datamodel import ECGBulk, ECG, HeartRate
import hr_detector


logger = logging.getLogger(__file__)

def fibonacci(n):
    if n <= 1:
        return n
    else:
        return fibonacci(n-1) + fibonacci(n-2)


class ECGWatcher(mp.Process):
    def __init__(self, queue: Queue, cfg_fpath: Union[Path, None] = None):
        super().__init__()
        self.redis = None
        self.queue: Queue[ECGBulk] = queue
        self.cfgpath = cfg_fpath
        self.last_ecg_idx = {}
        self.num_of_required_ecg = 24  # ecg interval is 2.5 sec, 2.5* 24 = 60 sec

    def run(self):
        self.redis = Redis.from_cfgfile(self.cfgpath)
        while not self.redis.is_connected:
            try:
                self.redis = Redis.from_cfgfile(self.cfgpath)
            except Exception as e:
                print_exc(e)

        while True:
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

                    ecgbulk: list[ECG] = []
                    for raw in map(json.loads, self.redis.mget(keys)):
                        ecg = ECG(device, raw['ts'], raw['ecg'])
                        ecgbulk.append(ecg)

                    self.queue.put(ECGBulk(ecgbulk))
                    logger.info(f'Device {device} ECG data is transfered')
                    # logger.info(f'Device {device} ECG data is transfered, ECG Queue Size: {self.queue.qsize()}')
                    continue

            time.sleep(.2)


class HeartRateCalculator(mp.Process):
    '''It calculates Heart Rates by using a multi-processing and put it to the outgoing queue.
    '''
    def __init__(self, incoming_queue: Queue, outgoing_queue: Queue):
        super().__init__()
        self.ecg_queue: Queue[ECGBulk] = incoming_queue
        self.hr_queue: Queue[HeartRate] = outgoing_queue
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
        return HeartRate(ecgbulk.device, int(time.time()), hr)


class HeartRateSender(mp.Process):
    def __init__(self, queue: Queue, cfg_fpath: Union[Path, None] = None):
        super().__init__()
        self.queue: Queue[HeartRate] = queue
        self.cfgpath = cfg_fpath

    def run(self):
        self.mqtt_client = MQTTClient.from_cfgfile(self.cfgpath)
        while not self.mqtt_client.is_connected:
            try:
                self.mqtt_client.connect()
            except:
                time.sleep(2)

        while True:
            try:
                if not self.queue.empty():
                    device, data = self.queue.get(True, 100)
                    print('HR', device)
                    self.mqtt_client.pub(
                        topic="v1/gateway/telemetry",
                        message= '{"0023A0000011": [{"ts": 1483228800000,"values": {"temperature": 42,"humidity": 80}},{"ts": 1483228801000,"values": {"temperature": 43,"humidity": 82}}],"0023P1000200": [{"ts": 1483228800000,"values": {"temperature": 42,"humidity": 80}}]}'
            )
            except KeyboardInterrupt:
                self.mqtt_client.disconnect()
            time.sleep(.2)
