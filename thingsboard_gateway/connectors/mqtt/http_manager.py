# HTTP Request Manager

import re
import asyncio
from multiprocessing import SimpleQueue
from signal import signal, SIGINT
from threading import Thread, RLock
import aiohttp
import logging
import json
import random
from time import sleep, time
import concurrent.futures
import requests

log = logging.getLogger("http")

HTTP_REQUEST_DELAY_SEC = 0.2
IOMT_JWT = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjb2xsZWN0aW9uSWQiOiJfcGJfdXNlcnNfYXV0aF8iLCJleHAiOjE3NDEzOTM2NTksImlkIjoiNWxjcWJjNXd1amZ1OXZwIiwidHlwZSI6ImF1dGhSZWNvcmQifQ._6EopNSD_yecWpn_qrP8J7wU_ZoM86JOK1Z1sOFMPwQ'
UPLOAD_URL = "https://iomt.karina-huinno.tk/iomt-api/examinations/upload-source-data"
TRIGGER_BASE_URL = "https://iomt.karina-huinno.tk/iomt-api/"


regex_uuid = re.compile("^[0-9a-f]{8}-[0-9a-f]{4}-[4-9a-f]{4}-[89ab-cd ef]{3}-[0-9a-f]{12}$")


async def _upload_ecg(device_name, ecg_values):
    async with aiohttp.ClientSession() as session:
        body = {
            "serialNumber": device_name,
            "requestTimestamp": int(time()),
            "requestSeconds": 60,
            "ecgData": ecg_values
        }
        payload = json.dumps(body)
        headers = {
            'Content-Type': 'application/json',
            'iomt-jwt': IOMT_JWT
        }
        async with session.post(UPLOAD_URL, headers=headers, data=payload) as response:
            print("url:", UPLOAD_URL)
            print("Status:", response.status)
            print("Content-type:", response.headers['content-type'])
            html = await response.text()
            print("Body:", html[:30], "...")


# class Singleton(type):
#     _instances = {}
#
#     def __call__(cls, *args, **kwargs):
#         if cls not in cls._instances:
#             cls._instances[cls] = super(Singleton, cls) \
#                 .__call__(*args, **kwargs)
#         return cls._instances[cls]
#
#
# class HttpManager(metaclass=Singleton):
class HttpManager:
    def __init__(self, ai_queue, trigger_queue):
        print('start HTTP Manager')
        self.id = random.randint(0, 10000)
        self.stopped = False
        self.__async_queue = ai_queue
        self.__trigger_queue = trigger_queue
        self.__loop = asyncio.new_event_loop()
        # start HTTP request handler
        self._http_ai_thread = Thread(target=self._send_ecg_to_ai, daemon=True, name="Send ECG to AI Server")
        self._http_ai_thread.start()
        self._http_noti_thread = Thread(target=self._trigger_noti_to_server, daemon=True, name="Trigger HTTP")
        self._http_noti_thread.start()
        print('HTTP Manager started')

    def __stop_thread(self):
        self.stopped = True
        log.info("Stopping...")

    def _send_ecg_to_ai(self):
        print("Send HTTP AI sending Thread has been started successfully.")

        while not self.stopped:
            if not self.__async_queue.empty():
                self.in_progress = True
                device_name, ecg_values = self.__async_queue.get()
                _upload_ecg(device_name, ecg_values)
                self.in_progress = False
            else:
                sleep(.2)

    def trigger_http(self, url_path, body):
        self.__trigger_queue.put((url_path, body))

    def upload_ecg(self, device_name, ecg_values):
        self.__async_queue.put((device_name, ecg_values))

    @staticmethod
    async def _trigger_http(url_path, body):
        print('trigger_http:' + url_path)
        print('body:' + json.dumps(body))
        async with aiohttp.ClientSession() as session:
            payload = json.dumps(body)
            headers = {
                'Content-Type': 'application/json'
            }
            async with session.post(TRIGGER_BASE_URL + url_path, headers=headers, data=payload) as response:
                print("url:", TRIGGER_BASE_URL + url_path)
                print("Status:", response.status)
                print("Content-type:", response.headers['content-type'])
                html = await response.text()
                print("Body:", html[:30], "...")

    def _trigger_noti_to_server(self):
        print("Send HTTP Trigger Thread has been started successfully.")

        while not self.stopped:
            if not self.__trigger_queue.empty():
                self.in_progress = True
                url_path, body = self.__trigger_queue.get()
                self._trigger_http(url_path, body)
                self.in_progress = False
            else:
                sleep(.2)
