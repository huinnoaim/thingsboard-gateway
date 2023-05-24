from __future__ import annotations
import builtins
import sys
import argparse
import os
import logging
from typing import NamedTuple
import multiprocessing as mp
from pathlib import Path

import yaml
from dotenv import load_dotenv

from modules.heartrate.workers import ECGWatcher, HeartRateCalculator, HeartRateSender, ECGUploader
from modules.heartrate.datamodel import HeartRate, ECGBulk

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)

class Envs(NamedTuple):
    REDIS_URL: str
    REDIS_PORT: int
    REDIS_PASSWORD: str
    MQTT_THINGSBOARD_URL: str
    MQTT_THINGSBOARD_PORT: int
    MQTT_THINGSBOARD_ACCESS_TOKEN: str
    MQTT_MOSQUITTO_URL: str
    MQTT_MOSQUITTO_PORT: int
    AI_SERVER_URL: str
    AI_SERVER_ACCESS_TOKEN: str

    @staticmethod
    def getenv(fpath: Path| None = None) -> Envs:
        load_dotenv(fpath)
        kwargs = {}
        for field in  Envs.__dict__['_fields']:
            casting = getattr(builtins, Envs.get_typing(field), None)
            value = os.getenv(field)
            kwargs[field] = casting(value) if casting else value
        return Envs(**kwargs)

    @classmethod
    def get_typing(cls, field: str) -> str:
        return cls.__annotations__[field].__forward_arg__


def update_cfgfile(envs: Envs, cfg_fpath: Path):
    '''It updates the config yaml file by using envs.
    '''
    with open(cfg_fpath, 'r') as f:
        yaml_data = yaml.load(f, Loader=yaml.SafeLoader)
        yaml_data['mqtt']['thingsboard']['url'] = envs.MQTT_THINGSBOARD_URL
        yaml_data['mqtt']['thingsboard']['port'] = envs.MQTT_THINGSBOARD_PORT
        yaml_data['mqtt']['thingsboard']['accessToken'] = envs.MQTT_THINGSBOARD_ACCESS_TOKEN
        yaml_data['mqtt']['mosquitto']['url'] = envs.MQTT_MOSQUITTO_URL
        yaml_data['mqtt']['mosquitto']['port'] = envs.MQTT_MOSQUITTO_PORT

        yaml_data['redis']['url'] = envs.REDIS_URL
        yaml_data['redis']['port'] = envs.REDIS_PORT
        yaml_data['redis']['password'] = envs.REDIS_PASSWORD

        yaml_data['aiServer']['url'] = envs.AI_SERVER_URL
        yaml_data['aiServer']['accessToken'] = envs.AI_SERVER_ACCESS_TOKEN

    with open(cfg_fpath, 'w') as f:
        yaml.safe_dump(yaml_data, f)


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='ECG Converters')
    parser.add_argument('--env-fpath', '-e', default='.env', help='envfile file path', type=Path)
    parser.add_argument('--cfg-fpath', '-c', default='./config/client.yaml', help='config file path', type=Path)
    return parser.parse_args()


def main(args: argparse.Namespace):
    envs = Envs.getenv(args.env_fpath)
    if envs:
        update_cfgfile(envs, args.cfg_fpath)

    ecg_queue: mp.Queue[ECGBulk] = mp.Queue()
    ai_queue: mp.Queue[ECGBulk] = mp.Queue()
    hr_queue: mp.Queue[HeartRate] = mp.Queue()

    watcher = ECGWatcher(ecg_queue, ai_queue)
    watcher.start()

    calculator = HeartRateCalculator.from_cfgfile(ecg_queue, hr_queue, args.cfg_fpath)
    calculator.start()

    hr_sender = HeartRateSender(hr_queue)
    hr_sender.start()

    ecg_uploader = ECGUploader.from_cfgfile(ai_queue, args.cfg_fpath)
    ecg_uploader.start()

    while True:
        try:
            pass
        except KeyboardInterrupt:
            watcher.close()
            hr_sender.close()
            ecg_uploader.close()

            ecg_queue.close()
            ai_queue.close()
            hr_queue.close()
            sys.exit(0)
    #     # manager.start()
    #     # server = manager.get_server()
    #     # server.serve_forever()


if __name__ == "__main__":
    args = get_args()
    main(args)


