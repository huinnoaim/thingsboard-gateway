from __future__ import annotations
import sys
import multiprocessing as mp
import threading
import logging
import time
import dataclasses as dc
import json
from pathlib import Path
from os import path
from typing import Any, Union
from traceback import print_exc

import yaml
from redis import Redis as RedisBase, ConnectionError

logger = logging.getLogger(__name__)


DEFAULT_CONFIG_FILEPATH = "/config/redis.yaml"
FILTERED_TTL = 60
ECG_TTL = 90  # 90 sec
RAW_TTL = 45


@dc.dataclass
class EcgData:
    device: str
    ts: int
    idx: int
    values: list[float]

    @property
    def redis_key(self) -> str:
        """Key for Redis"""
        return f"ecg:{self.device}:{self.idx}"

    @property
    def redis_values(self) -> str:
        """Values for Redis"""
        return json.dumps({"ts": self.ts, "ecg": self.values}).encode("utf-8")

    @staticmethod
    def from_parsed_data(parsed_data: dict) -> Union[EcgData, None]:
        try:
            device_name = str(parsed_data["deviceName"])
            field_ts = int(parsed_data["telemetry"][0]["ts"])
            field_ecg = json.dumps(parsed_data["telemetry"][0]["values"]["ecgData"])
            field_ecg_index = int(parsed_data["telemetry"][0]["values"]["ecgDataIndex"])
            return EcgData(
                device=device_name,
                ts=field_ts,
                idx=field_ecg_index,
                values=json.loads(field_ecg),
            )
        except:
            logger.debug(
                f"Parsing Failed: {device_name} sent INVALID ECG format: {str(parsed_data)}"
            )
            return None


@dc.dataclass
class RawData:
    device: str
    ts: int
    data: dict

    @property
    def redis_key(self) -> str:
        return f"raw:{self.device}:{self.ts}"

    @property
    def redis_values(self) -> str:
        return json.dumps(self.data).encode("utf-8")

    @staticmethod
    def from_parsed_data(parsed_data: dict) -> Union[RawData, None]:
        try:
            device_name = str(parsed_data["deviceName"])
            return RawData(
                device=device_name,
                ts=int(time.time() * 1000),
                data=parsed_data,
            )
        except:
            logger.debug(
                f"Parsing Failed: {device_name} sent INVALID RAW format: {str(parsed_data)}"
            )
            return None


@dc.dataclass
class FilteredData:
    device: str
    hint: str
    data: dict

    @property
    def redis_key(self) -> str:
        return f"filtered:{self.hint}:{self.device}"

    @property
    def redis_values(self) -> str:
        return json.dumps(self.data).encode("utf-8")

    @staticmethod
    def from_parsed_data(parsed_data: dict, hint: str) -> Union[RawData, None]:
        if "deviceName" not in parsed_data:
            hint = hint if hint else 'NULL'
            device = int(time.time())
            return FilteredData(device=device, hint=hint, data=parsed_data)

        try:
            device_name = str(parsed_data["deviceName"])
            return FilteredData(
                device=device_name,
                hint=hint,
                data=parsed_data,
            )
        except:
            logger.debug(
                f"Parsing Failed: {device_name} sent INVALID Filtered format: {str(parsed_data)}"
            )
            return None


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

    @staticmethod
    def from_cfgfile(fpath: Union[Path, None] = None) -> Redis:
        dirname = path.dirname(path.dirname(path.abspath(__file__)))
        cfg_file = dirname + DEFAULT_CONFIG_FILEPATH.replace("/", path.sep)
        cfg_file = cfg_file if fpath is None else fpath

        with open(cfg_file) as general_config:
            cfg = yaml.safe_load(general_config)

        redis_cfg = cfg["redis"]
        host = redis_cfg["host"]
        port = redis_cfg["port"]
        password = redis_cfg["password"]
        return Redis(host=host, port=port, password=password)


class RedisSender(mp.Process):
    def __init__(
        self,
        raw_queue: mp.Queue,
        ecg_queue: mp.Queue,
        filtered_queue: mp.Queue,
        cfg_fpath: Union[Path, None] = None,
    ) -> None:
        super().__init__()
        self.raw_queue = raw_queue
        self.ecg_queue = ecg_queue
        self.filtered_queue: mp.Queue[(str, dict)] = filtered_queue
        self.name = "Redis Sender"
        self.cfgpath = cfg_fpath
        self.redis = None
        self.bulksize = 40

    def run(self):
        try:
            self.redis = Redis.from_cfgfile(self.cfgpath)  # redis is singletone
        except Exception as e:
            print_exc(e)
            raise RuntimeError("Run Redis is failed")

        ecg_thread = threading.Thread(
            target=self.ecg_sender, name="ECG Data Sender Thread"
        )
        raw_thread = threading.Thread(
            target=self.raw_sender, name="RAW Data Sender Thread"
        )
        filtered_thread = threading.Thread(
            target=self.filtered_sender, name="Filtered Data Sender Thread"
        )

        ecg_thread.start()
        raw_thread.start()
        filtered_thread.start()
        logger.info('Redis Sender Start')

        while True:
            try:
                pass
            except (KeyboardInterrupt, ConnectionError, ConnectionRefusedError) as e:
                print_exc()
                logger.info('Redis Sender Shutdown')
                self.redis.close()
                self.close()

    def ecg_sender(self):
        while True:
            if not self.ecg_queue.empty():
                ecgs: list[EcgData] = []
                for _ in range(min(self.bulksize, self.ecg_queue.qsize())):
                    parsed_data = self.ecg_queue.get(True, 100)
                    ecg_data = EcgData.from_parsed_data(parsed_data)
                    if ecg_data:
                        ecgs.append(ecg_data)

                with self.redis.pipeline() as pipe:
                    for ecg in ecgs:
                        pipe.set(ecg.redis_key, ecg.redis_values)
                        pipe.expire(ecg.redis_key, ECG_TTL)
                    pipe.execute()

                logger.info(
                    f"# {len(ecgs)} ECG data is sent to Redis, # Redis ECGQueueSize: {self.ecg_queue.qsize()}"
                )

            time.sleep(0.2)

    def raw_sender(self):
        while True:
            if not self.raw_queue.empty():
                raws: list[RawData] = []
                for _ in range(min(self.bulksize, self.raw_queue.qsize())):
                    parsed_data = self.raw_queue.get(True, 100)
                    raw_data = RawData.from_parsed_data(parsed_data)
                    if raw_data:
                        raws.append(raw_data)

                with self.redis.pipeline() as pipe:
                    for raw in raws:
                        pipe.set(raw.redis_key, raw.redis_values)
                        pipe.expire(raw.redis_key, RAW_TTL)
                    pipe.execute()

                logger.info(
                    f"# {len(raws)} RAW data is sent to Redis, # Redis RawQueueSize: {self.raw_queue.qsize()}"
                )

            time.sleep(0.2)

    def filtered_sender(self):
        while True:
            if not self.filtered_queue.empty():
                filtereds: list[FilteredData] = []
                for _ in range(min(self.bulksize, self.filtered_queue.qsize())):
                    hint, parsed_data = self.filtered_queue.get(True, 100)
                    filtered_data = FilteredData.from_parsed_data(parsed_data, hint)
                    if filtered_data:
                        filtereds.append(filtered_data)

                with self.redis.pipeline() as pipe:
                    for filtered in filtereds:
                        pipe.set(filtered.redis_key, filtered.redis_values)
                        pipe.expire(filtered.redis_key, FILTERED_TTL)
                    pipe.execute()

                logger.info(
                    f"# {len(filtereds)} Filtered data is sent to Redis, # Redis FilteredQueueSize: {self.filtered_queue.qsize()}"
                )

            time.sleep(0.2)
