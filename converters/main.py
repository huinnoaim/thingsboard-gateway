from __future__ import annotations
import argparse
import os
from typing import NamedTuple
import logging
import multiprocessing as mp
from pathlib import Path
# from queue import Queue
import yaml
from dotenv import load_dotenv

from workers import ECGWatcher, HeartRateCalculator
from datamodel import HeartRate, ECG, ECGBulk

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)

class Envs(NamedTuple):
    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_PASSWORD: str
    MQTT_HOST: str
    MQTT_PORT: int
    MQTT_ACCESS_TOKEN: str

    @staticmethod
    def getenv(fpath: Path| None = None) -> Envs:
        load_dotenv(fpath)
        kwargs = {}
        for field in  Envs.__dict__['_fields']:
            value = os.getenv(field)
            kwargs[field] = value
        return Envs(**kwargs)


def update_cfgfile(envs: Envs, cfg_fpath: Path):
    '''It updates the config yaml file by using envs.
    '''
    with open(cfg_fpath, 'r') as f:
        yaml_data = yaml.load(f, Loader=yaml.SafeLoader)
        yaml_data['thingsboard']['host'] = envs.MQTT_HOST
        yaml_data['thingsboard']['port'] = int(envs.MQTT_PORT)
        yaml_data['thingsboard']['security']['accessToken'] = envs.MQTT_ACCESS_TOKEN
        yaml_data['redis']['host'] = envs.REDIS_HOST
        yaml_data['redis']['port'] = int(envs.REDIS_PORT)
        yaml_data['redis']['password'] = envs.REDIS_PASSWORD

    with open(cfg_fpath, 'w') as f:
        yaml.safe_dump(yaml_data, f)


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='ECG Converters')
    parser.add_argument('--envfpath', '-e', default='.env', help='envfile file path', type=Path)
    parser.add_argument('--cfgfpath', '-c', default='./config/client.yaml', help='config file path', type=Path)
    return parser.parse_args()


def main():
    ecg_queue: mp.Queue[list[ECG]] = mp.Queue()
    hr_queue: mp.Queue[HeartRate] = mp.Queue()

    watcher = ECGWatcher(ecg_queue)
    watcher.start()

    calculator = HeartRateCalculator(incoming_queue=ecg_queue, outgoing_queue=hr_queue)
    calculator.start()

    while True:
        pass
        # manager.start()
        # server = manager.get_server()
        # server.serve_forever()


if __name__ == "__main__":
    args = get_args()
    envs = Envs.getenv(args.envfpath)
    if envs:
        update_cfgfile(envs, args.cfgfpath)

    main()


