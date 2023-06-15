#     Copyright 2022. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
import argparse
import os
from os import path
from pathlib import Path

import yaml


def update_cfg(cfg_fpath: Path, host: str, port: int, access_token: str):
    with open(cfg_fpath, 'r') as f:
        yaml_data = yaml.load(f, Loader=yaml.SafeLoader)
    yaml_data['thingsboard']['host'] = host
    yaml_data['thingsboard']['port'] = port
    yaml_data['thingsboard']['security']['accessToken'] = access_token

    with open(cfg_fpath, 'w') as f:
        yaml.safe_dump(yaml_data, f)


def main(args: argparse.Namespace):
    update_cfg(args.cfg_fpath, args.host, args.port, args.access_token)
    os.system('python -m thingsboard_gateway.tb_gateway')


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Thingsboard Gateway EntryPoint. It updates cfg file and execute the tb gateway")
    parser.add_argument("--host", help="Thingsboard MQTT Host Address", required=True)
    parser.add_argument("--port", help="Thingsboard MQTT Host port", required=True, type=int)
    parser.add_argument("--access_token", help="Access Token For Accessing The MQTT Host", required=True)
    parser.add_argument(
        "--cfg_fpath",
        help="Thingsboard Gateway Configuration Filepath",
        default=path.dirname(path.abspath(__file__)) + "/config/tb_gateway.yaml".replace("/", path.sep)
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    main(args)
