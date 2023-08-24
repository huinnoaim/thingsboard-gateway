from __future__ import annotations
import builtins
import argparse
import os
import logging
from typing import NamedTuple
import json
from pathlib import Path

import yaml
from dotenv import load_dotenv
import paho.mqtt.client as mqtt

from connectors import MQTTClient
from alarm.alarm_manager import AlarmManager


logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)


class Envs(NamedTuple):
    MQTT_MOSQUITTO_URL: str
    MQTT_MOSQUITTO_PORT: int

    @staticmethod
    def getenv(fpath: Path | None = None) -> Envs:
        load_dotenv(fpath)
        kwargs = {}
        for field in Envs.__dict__["_fields"]:
            casting = getattr(builtins, Envs.get_typing(field), None)
            value = os.getenv(field)
            kwargs[field] = casting(value) if casting else value
        return Envs(**kwargs)

    @classmethod
    def get_typing(cls, field: str) -> str:
        return cls.__annotations__[field].__forward_arg__


def update_cfgfile(envs: Envs, cfg_fpath: Path):
    """It updates the config yaml file by using envs."""
    with open(cfg_fpath, "r") as f:
        yaml_data = yaml.load(f, Loader=yaml.SafeLoader)
        yaml_data["mqtt"]["mosquitto"]["url"] = envs.MQTT_MOSQUITTO_URL
        yaml_data["mqtt"]["mosquitto"]["port"] = envs.MQTT_MOSQUITTO_PORT

    with open(cfg_fpath, "w") as f:
        yaml.safe_dump(yaml_data, f)


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ECG Converters")
    parser.add_argument(
        "--env-fpath", "-e", default=".env", help="envfile file path", type=Path
    )
    parser.add_argument(
        "--cfg-fpath",
        "-c",
        default="./config.yaml",
        help="config file path",
        type=os.path.abspath,
    )
    return parser.parse_args()


def main(args: argparse.Namespace):
    envs = Envs.getenv(args.env_fpath)
    if envs:
        update_cfgfile(envs, args.cfg_fpath)
    logger.info(envs)
    logger.info(args.cfg_fpath)

    client = MQTTClient.from_cfgfile("mosquitto", args.cfg_fpath)
    __alarm_manager = AlarmManager(client)

    def handle_hr_message(client, userdata, msg):
        payload = msg.payload.decode()
        payload_slice = payload.split(":")
        params = payload_slice[1]
        params_slice = params.split(",")
        sensor_type = payload_slice[0][5:]
        value = params_slice[1]

        __alarm_manager.check_alarm(params_slice[0].split("=")[1], sensor_type, value)

    def handle_alarm_message(client, userdata, msg):
        payload = json.loads(msg.payload.decode())
        __alarm_manager.upsert_alarm(msg.topic, payload)

    def handle_alarm_rule_message(client, userdata, msg):
        payload = json.loads(msg.payload.decode())
        if "from" in payload and payload["from"] == "pmc":
            __alarm_manager.get_alarm_rule()
        else:
            __alarm_manager.upsert_alarm_rule(payload)

    def handle_reload_message(client, userdata, msg):
        __alarm_manager.get_exam_with_serial_number()

    topic_handlers = {
        "devices/hr": handle_hr_message,
        "alarms/#": handle_alarm_message,
        "noti/alarm-rules/#": handle_alarm_rule_message,
        "noti/reload": handle_reload_message,
    }

    def on_message(client, userdata, msg):
        topic = msg.topic
        for topic_pattern, handler in topic_handlers.items():
            if mqtt.topic_matches_sub(topic_pattern, topic):
                handler(client, userdata, msg)

    client.connect()

    for topic in topic_handlers:
        client.sub(topic)

    client.client.on_message = on_message

    try:
        while True:
            pass

    except:
        client.disconnect()


if __name__ == "__main__":
    args = get_args()
    main(args)
