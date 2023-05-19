from __future__ import annotations
from typing import Union
from pathlib import Path
import os

import yaml
import paho.mqtt.client as mqtt


DEFAULT_CFG_PATH = '/config/client.yaml'


class MQTTClient:
    def __init__(self, url: str, port: int, token: str):
        self.url = url
        self.port = port
        self.token = token
        self.client = mqtt.Client()

    def on_connect(self, client, userdata, flags, rc):
        print(f"Connected with result code {rc}")

    def on_publish(self, client, userdata, mid):
        print("Message published")



    def connect(self):
        self.client.on_connect = self.on_connect
        self.client.on_publish = self.on_publish

        if self.token:
            self.client.username_pw_set(self.token, "")
        self.client.connect(self.url, self.port)
        self.client.loop_start()

    def pub(self, topic: str, message: str, qos=1):
        self.client.publish(topic, message, qos)

    def sub(self, topic, qos=1):
        self.client.subscribe(topic, qos)

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()

    @property
    def is_connected(self) -> bool:
        return self.client.is_connected()

    @staticmethod
    def from_cfgfile(fpath: Union[Path, None] = None) -> MQTTClient:
        dirname = os.path.dirname(os.path.abspath(__file__))
        cfg_file = dirname + DEFAULT_CFG_PATH.replace('/', os.path.sep)
        cfg_file = cfg_file if fpath is None else fpath

        with open(cfg_file) as general_config:
            full_cfg = yaml.safe_load(general_config)

        cfg = full_cfg['thingsboard']
        host = cfg['host']
        port = cfg['port']
        access_token = cfg['security']['accessToken']
        return MQTTClient(host=host, port=port, token=access_token)
