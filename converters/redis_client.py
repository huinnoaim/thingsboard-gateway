from __future__ import annotations
import threading
import logging
from pathlib import Path
from os import path
import yaml
from typing import Union

from redis import Redis as RedisBase


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)


DEFAULT_CFG_PATH = '/config/client.yaml'

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
        dirname = path.dirname(path.abspath(__file__))
        cfg_file = dirname + DEFAULT_CFG_PATH.replace('/', path.sep)
        cfg_file = cfg_file if fpath is None else fpath

        with open(cfg_file) as general_config:
            full_cfg = yaml.safe_load(general_config)

        cfg = full_cfg['redis']
        host = cfg['host']
        port = cfg['port']
        password = cfg['password']
        return Redis(host=host, port=port, password=password)


class RedisUtils:

    class ECG:
        @staticmethod
        def get_devices(redis: Redis) -> list[str]:
            ptrn = ptrn = f'ecg:*'  # ecg:{device}:{ecgIndex}
            keys = redis.keys(ptrn)
            extract_device = lambda x: x.decode('utf-8').split(':')[1]
            devices = map(extract_device, keys)
            return sorted(set(devices))

        def get_lastest_index(redis: Redis, device: str, latest: Union[int, None] = None) -> list[str]:
            ptrn = ptrn = f'ecg:{device}:*'  # ecg:{device}:{ecgIndex}
            keys = map(lambda x: x.decode('utf-8'), redis.keys(ptrn))
            sortby_index = lambda x: int(x.split(':')[2])
            return sorted(keys, reverse=True, key=sortby_index)[:latest]
