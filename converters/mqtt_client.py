from __future__ import annotations
import time
import yaml
import threading
from os import path
from pathlib import Path
from typing import Union
import logging
import dataclasses as dc

from tb_device_mqtt import TBDeviceMqttClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)


def get_client(fpath: Union[Path, None] = None) -> TBMqttClient:
    dirname = path.dirname(path.abspath(__file__))
    cfg_file = dirname + '/config/tb_client.yaml'.replace('/', path.sep)
    cfg_file = cfg_file if fpath is None else fpath

    with open(cfg_file) as general_config:
        cfg = yaml.safe_load(general_config)

    tb_cfg = cfg['thingsboard']
    host = tb_cfg['host']
    port = tb_cfg['port']
    security_token = tb_cfg['security']['accessToken']
    return TBMqttClient(host, port, security_token)


@dc.dataclass
class DeviceData:
    device: Union[str, None]
    attribute: Union[str, None]
    telemetry: Union[str, None]


class SingletonType(type):
    '''It restricts a class as Singleton.

    Refer the belows:
    1. https://blog.ionelmc.ro/2015/02/09/understanding-python-metaclasses/
    2. https://dojang.io/mod/page/view.php?id=2468
    '''
    def __call__(cls, *args, **kwargs):
        try:
            return cls.__instance
        except AttributeError:
            cls.__instance = super().__call__(*args, **kwargs)
            return cls.__instance


class TBMqttClient(threading.Thread, metaclass=SingletonType):
    '''Thingsboard MQTT Client.

    Refer the belows:
    1. https://thingsboard.io/docs/reference/python-client-sdk/
    2. https://thingsboard.io/docs/reference/mqtt-api/
    '''
    def __init__(self, host: str, port: int, security_token: str) -> None:
        super().__init__()
        self.setName = 'Thingsboard MQTT client'
        self.setDaemon = True

        self.__stopped = False
        self.client = TBDeviceMqttClient(host=host, port=port, username=security_token)
        self.start()

    def send_bulk(self, bulkdata: list[DeviceData]):
        '''It sends bulk data to Thingsboard Server.
        '''
        for data in bulkdata:
            self.client.send_telemetry(data.telemetry)
            self.client.send_attributes(data.attribute)

    def run(self):
        try:
            while not self.client.is_connected() and not self.__stopped:
                if self.__stopped:
                    break
                logger.info("connecting to ThingsBoard")
                try:
                    self.client.connect()
                    logger.debug('connected')
                except ConnectionRefusedError as e:
                    logger.error('ConnectionRefusedError', e)
                    pass
                except Exception as e:
                    logger.exception(e)
                time.sleep(2)
        except Exception as e:
            logger.exception(e)
            time.sleep(10)

        while not self.__stopped:
            try:
                if not self.__stopped:
                    time.sleep(.2)
                else:
                    break
            except KeyboardInterrupt:
                self.__stopped = True
            except Exception as e:
                logger.exception(e)
