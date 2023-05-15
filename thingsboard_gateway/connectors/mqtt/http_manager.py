# HTTP Request Manager

import requests
import json
import logging
import random
import re
from threading import Thread
from time import sleep, time

import aiohttp

log = logging.getLogger("http")

HTTP_REQUEST_DELAY_SEC = 0.2
IOMT_JWT = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjb2xsZWN0aW9uSWQiOiJfcGJfdXNlcnNfYXV0aF8iLCJleHAiOjE3NDEzOTM2NTksImlkIjoiNWxjcWJjNXd1amZ1OXZwIiwidHlwZSI6ImF1dGhSZWNvcmQifQ._6EopNSD_yecWpn_qrP8J7wU_ZoM86JOK1Z1sOFMPwQ'
UPLOAD_URL = "https://iomt.karina-huinno.tk/iomt-api/examinations/upload-source-data"
TRIGGER_BASE_URL = "https://iomt.karina-huinno.tk/iomt-api/"


regex_uuid = re.compile("^[0-9a-f]{8}-[0-9a-f]{4}-[4-9a-f]{4}-[89ab-cd ef]{3}-[0-9a-f]{12}$")


def _upload_ecg(device_name, ecg_values):
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
    response = requests.post(UPLOAD_URL, headers=headers, data=payload)
    print("url:", UPLOAD_URL)
    print("Status:", response.status_code)
    print("Content-type:", response.headers['content-type'])
    html = response.text
    print("Body:", html[:30], "...")


class HttpManager:
    def __init__(self, ai_queue, trigger_queue):
        print('start HTTP Manager')
        self.id = random.randint(0, 10000)
        self.stopped = False
        self.__async_queue = ai_queue
        self.__trigger_queue = trigger_queue
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
        while True:
            if not self.__async_queue.empty():
                device_name, ecg_values = self.__async_queue.get()
                _upload_ecg(device_name, ecg_values)
            else:
                sleep(.2)

    def trigger_http(self, url_path, body):
        self.__trigger_queue.put((url_path, body))

    def upload_ecg(self, device_name, ecg_values):
        self.__async_queue.put((device_name, ecg_values))

    def _trigger_noti_to_server(self):
        while True:
            if not self.__trigger_queue.empty():
                url_path, body = self.__trigger_queue.get()
                headers = {
                    'Content-Type': 'application/json'
                }
                with requests.Session() as session:
                    response = session.post(TRIGGER_BASE_URL + url_path, headers=headers, data=json.dumps(body))
                    print("url:", TRIGGER_BASE_URL + url_path)
                    print("Status:", response.status_code)
                    print("Content-type:", response.headers['content-type'])
                    html = response.text
                    print("Body:", html[:30], "...")
            else:
                sleep(.2)
