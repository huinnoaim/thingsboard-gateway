from __future__ import annotations
from typing import Union
from pathlib import Path
import time
import os
import logging
import threading

import yaml
import paho.mqtt.client as mqtt

from connectors import DEFAULT_CFG_DIRPATH


logger = logging.getLogger(__name__)


RESULT_CODES = {
    1: "incorrect protocol version",
    2: "invalid client identifier",
    3: "server unavailable",
    4: "bad username or password",
    5: "not authorised",
}


class MQTTClient(threading.Thread):
    def __init__(
        self,
        hostname: str,
        url: str,
        port: int,
        token: Union[str, None],
        keep_alive: int = 120,
        min_reconnect_delay: int = 10,
        timeout: int = 120,
    ):
        super().__init__()
        self.setName(f"{hostname} MQTT Connector")
        self.isDaemon = True
        self.hostname: str = hostname
        self.url: str = url
        self.port: int = port
        self.token: str = token
        self.client = mqtt.Client()
        self._is_connected: bool = False
        self.keep_alive: int = keep_alive
        self.min_reconnect_delay: int = min_reconnect_delay
        self.timeout: int = timeout

        self.client._on_connect = self.on_connect
        self.client._on_disconnect = self.disconnect
        self.client._on_publish = self.on_publish

    def on_connect(self, client, userdata, flags, rc, *args, **kwargs):
        if rc == 0:
            logger.info(f"Connected with result code {rc}")
        else:
            if rc in RESULT_CODES:
                logger.error(f"connection FAIL with error {rc} {RESULT_CODES[rc]}")
            else:
                logger.error("connection FAIL with unknown error")

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()

    def on_publish(self, client, userdata, mid):
        logger.info(f"Message published to {self.hostname}")

    def connect(self):
        if self.token:
            self.client.username_pw_set(self.token, "")

        try:
            self.client.connect(
                host=self.url, port=self.port, keepalive=self.keep_alive
            )
            self.client.reconnect_delay_set(self.min_reconnect_delay, self.timeout)
            self.client.loop_start()
        except ValueError as e:
            raise ValueError(e)

    def pub(self, topic: str, message: str, qos=1):
        self.client.publish(topic, message, qos)

    def sub(self, topic, qos=1):
        self.client.subscribe(topic, qos)

    def run(self):
        logger.info(f"Start {self.hostname} MQTT Connector")
        while not self.client.is_connected():
            logger.info(f"connecting to {self.hostname}: {self.url}:{self.port}")
            try:
                self.connect()
                logger.info(f"connected to {self.hostname}")
            except ConnectionRefusedError as e:
                logger.error(f"{self.hostname}, MQTT Broker ConnectionRefusedError", e)
                pass
            except ValueError as e:
                logger.error(f"{self.hostname}, Invalid MQTT Broker Configuration", e)
                break
            time.sleep(1)

    @property
    def is_connected(self) -> bool:
        return self.client.is_connected()

    @staticmethod
    def from_cfgfile(
        hostname: str = "localhost", fpath: Union[Path, None] = None
    ) -> MQTTClient:
        cfg_file = os.path.join(DEFAULT_CFG_DIRPATH, "client.yaml")
        cfg_file = cfg_file if fpath is None else fpath

        with open(cfg_file) as general_config:
            full_cfg = yaml.safe_load(general_config)

        cfg: dict = full_cfg["mqtt"][hostname]
        url = cfg["url"]
        port = cfg["port"]
        access_token = cfg.get("accessToken", None)
        keep_alive = cfg["connection"]["keepAlive"]
        min_reconnect_delay = cfg["connection"]["minReconnectDelay"]
        timeout = cfg["connection"]["timeout"]
        return MQTTClient(
            hostname=hostname,
            url=url,
            port=port,
            token=access_token,
            keep_alive=keep_alive,
            min_reconnect_delay=min_reconnect_delay,
            timeout=timeout,
        )
