from __future__ import annotations
import multiprocessing as mp
import logging
import time
import yaml
import dataclasses as dc
import json
from pathlib import Path
from os import path
from typing import Any, Union

from redis import Redis as RedisBase


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)


DEFAULT_CONFIG_FILEPATH = "/config/redis.yaml"
REDIS_TTL = 300  # 300 sec

@dc.dataclass
class EcgData:
    device: str
    ts: int
    idx: int
    values: list[float]

    @property
    def redis_key(self) -> str:
        '''Key for Redis'''
        return f'ecg:{self.device}:{self.idx}'

    @property
    def redis_values(self) -> str:
        '''Values for Redis'''
        return json.dumps({'ts': self.ts, 'ecg': self.values}).encode('utf-8')

    @staticmethod
    def from_telemetry(telemetry_data: dict) -> EcgData:
        device_name = str(telemetry_data['deviceName'])
        field_ts = int(telemetry_data['telemetry'][0]['ts'])
        field_ecg = json.dumps(telemetry_data['telemetry'][0]['values']['ecgData'])
        field_ecg_index = int(telemetry_data['telemetry'][0]['values']['ecgDataIndex'])
        return EcgData(device=device_name, ts=field_ts, idx=field_ecg_index, values=json.loads(field_ecg))


class SingletonType(type):
    '''It restricts a class as Singleton.

    Refer belows:
    1. https://blog.ionelmc.ro/2015/02/09/understanding-python-metaclasses/
    2. https://dojang.io/mod/page/view.php?id=2468
    '''
    def __call__(cls, *args, **kwargs):
        try:
            return cls.__instance
        except AttributeError:
            cls.__instance = super().__call__(*args, **kwargs)
            return cls.__instance


class Redis(RedisBase):
    __metaclass__ = SingletonType
    '''Redis Singletone Base Class.
    '''

    @staticmethod
    def from_cfgfile(fpath: Union[Path, None] = None) -> Redis:
        dirname = path.dirname(path.dirname(path.abspath(__file__)))
        cfg_file = dirname + DEFAULT_CONFIG_FILEPATH.replace('/', path.sep)
        cfg_file = cfg_file if fpath is None else fpath

        with open(cfg_file) as general_config:
            cfg = yaml.safe_load(general_config)

        redis_cfg = cfg['redis']
        host = redis_cfg['host']
        port = redis_cfg['port']
        password = redis_cfg['password']
        return Redis(host=host, port=port, password=password)

class RedisSender(mp.Process):
    def __init__(self, queue: mp.Queue, cfg_fpath: Union[Path, None] = None) -> None:
        super().__init__()
        self.queue = queue
        self.name = 'Redis Sender'
        self.cfgpath = cfg_fpath
        self.redis = None

    def run(self):
        self.redis = Redis.from_cfgfile(self.cfgpath)  # redis is singletone
        while True:
            if not self.queue.empty():
                converted_data = self.queue.get(True, 100)
                ecg_data = EcgData.from_telemetry(converted_data)
                logger.info(f'{ecg_data.device} ECG data is sent to Redis')
                self.redis.set(ecg_data.redis_key, ecg_data.redis_values)
                self.redis.expire(ecg_data.redis_key, REDIS_TTL)

                # result: bytes = self.redis.get(ecg_data.redis_key)
                # result = json.loads(result.decode('utf-8'))
                # print(result)

            time.sleep(.2)

