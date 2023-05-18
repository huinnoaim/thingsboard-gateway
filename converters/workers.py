import time
import json
import multiprocessing as mp
from typing import Union, NamedTuple
from pathlib import Path

from redis_client import Redis, RedisUtils


def fibonacci(n):
    if n <= 1:
        return n
    else:
        return fibonacci(n-1) + fibonacci(n-2)


class QueuedECG(NamedTuple):
    device: str
    data: list[dict]

class ECGWatcher(mp.Process):
    def __init__(self, queue: mp.Queue, cfg_fpath: Union[Path, None] = None):
        super().__init__()
        self.redis = None
        self.queue = queue
        self.cfgpath = cfg_fpath
        self.last_ecg_idx = {}
        self.num_of_required_ecg = 10

    def run(self):
        self.redis = Redis.from_cfgfile(self.cfgpath)
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
                    data = [json.loads(self.redis.get(key)) for key in latests]
                    self.queue.put(QueuedECG(device, data))
                    print('ECG', device)
                    continue

            time.sleep(.2)


class HRCalculator(mp.Process):
    def __init__(self, queue: mp.Queue, cfg_fpath: Union[Path, None] = None):
        super().__init__()
        self.queue: mp.Queue[QueuedECG] = queue
        self.cfgpath = cfg_fpath

    def run(self):
        while True:
            if not self.queue.empty():
                device, data = self.queue.get(True, 100)
                print('HR', device)
                fibonacci(30)

            time.sleep(.2)

    @staticmethod
    def calculate_hr(device_name):
        # # calculate HR
        # # now_ms = int( time.time_ns() / 1000 )
        # now_sec = int(time.time())
        # hr_input = filter(lambda d: (d[2] == device_name and (int(d[0]) >= now_sec - HR_CALC_RANGE_SEC)), list(ecg_cache.items()))
        # # print(len(list(hr_input)))
        # # print(list(hr_input))
        # # hr_input tuple = (timestamp, ecg, device_name)
        # hr_input = sorted(hr_input, key=lambda el: el[0], reverse=False)
        # # print(list(map(lambda d: d[0], hr_input)))
        # hr_input = list(map(lambda d: json.loads(d[1]), hr_input))
        # hr_input = np.array(hr_input, float).flatten().tolist()
        # # 3200개 / 5 번, 11초  = 640개/1번 = 320개/1초
        # # 2560개 / 4 번, 10초 = 640개/1번 = 약256개/1초
        # # print(len(hr_input))
        # # print(list(hr_input))
        # # 250 samples/s
        # hr = hr_detector.detect(hr_input, 250)
        # log.debug(device_name + ', HR: ' + str(hr))
        # return hr
        return None
