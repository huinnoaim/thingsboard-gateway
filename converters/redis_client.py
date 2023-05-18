from __future__ import annotations
import logging
from pathlib import Path
from os import path
import yaml
from typing import Union

from redis import Redis as RedisBase


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)


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
        cfg_file = dirname + '/config/client.yaml'.replace('/', path.sep)
        cfg_file = cfg_file if fpath is None else fpath

        with open(cfg_file) as general_config:
            cfg = yaml.safe_load(general_config)

        redis_cfg = cfg['redis']
        host = redis_cfg['host']
        port = redis_cfg['port']
        password = redis_cfg['password']
        return Redis(host=host, port=port, password=password)



if __name__ == "__main__":
    redis = Redis.from_cfgfile()
    redis.rpush('test', 1)
