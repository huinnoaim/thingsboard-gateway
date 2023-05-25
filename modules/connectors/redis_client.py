from __future__ import annotations
import os
import logging
from pathlib import Path
from os import path
from typing import Union

import yaml
from redis import ConnectionError, Redis as RedisBase

from connectors import DEFAULT_CFG_DIRPATH


logger = logging.getLogger(__name__)

DEFAULT_CFG_PATH = "/config/client.yaml"


class SingletonType(type):
    """It restricts a class as Singleton.

    Refer belows:
    1. https://blog.ionelmc.ro/2015/02/09/understanding-python-metaclasses/
    2. https://dojang.io/mod/page/view.php?id=2468
    """

    def __call__(cls, *args, **kwargs):
        try:
            return cls.__instance
        except AttributeError:
            cls.__instance = super().__call__(*args, **kwargs)
            return cls.__instance


class Redis(RedisBase):
    __metaclass__ = SingletonType
    """Redis Singletone Base Class.
    """

    @property
    def is_connected(self):
        try:
            self.ping()
            logger.info("Successfully connected to redis")
        except (ConnectionError, ConnectionRefusedError):
            logger.info("Redis connection error!")
            return False
        return True

    @staticmethod
    def from_cfgfile(fpath: Union[Path, None] = None) -> Redis:
        cfg_file = os.path.join(DEFAULT_CFG_DIRPATH, "client.yaml")
        cfg_file = cfg_file if fpath is None else fpath

        with open(cfg_file) as general_config:
            full_cfg = yaml.safe_load(general_config)

        cfg = full_cfg["redis"]
        url = cfg["url"]
        port = cfg["port"]
        password = cfg["password"]
        return Redis(host=url, port=port, password=password)


class RedisUtils:
    class ECG:
        @staticmethod
        def get_devices(redis: Redis) -> list[str]:
            ptrn = ptrn = f"ecg:*"  # ecg:{device}:{ecgIndex}
            keys = redis.keys(ptrn)
            extract_device = lambda x: x.decode("utf-8").split(":")[1]
            devices = map(extract_device, keys)
            return sorted(set(devices))

        def get_lastest_index(
            redis: Redis, device: str, latest: Union[int, None] = None
        ) -> list[str]:
            ptrn = ptrn = f"ecg:{device}:*"  # ecg:{device}:{ecgIndex}
            keys = map(lambda x: x.decode("utf-8"), redis.keys(ptrn))
            sortby_index = lambda x: int(x.split(":")[2])
            return sorted(keys, reverse=True, key=sortby_index)[:latest]

    class RAW:
        @staticmethod
        def get_devices(redis: Redis) -> list[str]:
            ptrn = ptrn = f"raw:*"  # raw:{device}:{ts}
            keys = redis.keys(ptrn)
            extract_device = lambda x: x.decode("utf-8").split(":")[1]
            devices = map(extract_device, keys)
            return sorted(set(devices))
