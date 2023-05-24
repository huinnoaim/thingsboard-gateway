import logging
import uuid
import json
from enum import Enum

import requests

from modules.connectors import MQTTClient


logger = logging.getLogger(__file__)

class AlarmStatus(Enum):
    ACK = 'ACTIVE_ACK'
    UNACK = 'ACTIVE_UNACK'
    CLEARED = 'CLEARED_ACK'

class AlarmSeverity(Enum):
    CRITICAL = 'CRITICAL'
    MAJOR = 'MAJOR'
    MINOR = 'MINOR'
class AlarmManager:
    def __init__(self, client:MQTTClient):
        self.alarm_rule = []
        self.exam_serial = []
        self.last_alarm_list = []
        self.client = client
        self.get_alarm_rule()
        self.get_exam_with_serial_number()

    def get_alarm_rule(self):
        api_url = 'https://iomt.karina-huinno.tk/webhook/alarm_rule'
        headers = {
            'Content-Type': 'application/json',
        }
        try:
            response = requests.get(api_url, headers=headers)
            if response.status_code == 200:
                self.alarm_rule = response.json()
            else:
                logger.error(f'Failed to get alarm rule. Status code: {response.status_code}')
        except requests.exceptions.RequestException as e:
            logger.error(f'Error during API call: {e}')

    def get_exam_with_serial_number(self):
        api_url = 'https://iomt.karina-huinno.tk/webhook/exam_list'
        headers = {
            'Content-Type': 'application/json',
        }
        try:
            response = requests.get(api_url, headers=headers)
            if response.status_code == 200:
                self.exam_serial = response.json()
            else:
                logger.error(f'Failed to exam with serial. Status code: {response.status_code}')
        except requests.exceptions.RequestException as e:
            logger.error(f'Error during API call: {e}')

    def upsert_alarm(self, topic, payload):
        topic_parts = topic.split('/')
        if len(topic_parts) != 4 or topic_parts[0] != 'alarms':
            logger.error('Invalid topic format')

        hospital_id, ward_id, exam_id = topic_parts[1], topic_parts[2], topic_parts[3]

        if not hospital_id or not ward_id or not exam_id:
            logger.error('Invalid topic - hospital_id, ward_id, or exam_id is missing')

        api_url = 'https://iomt.karina-huinno.tk/webhook/alarms'
        headers = {
            'Content-Type': 'application/json',
        }

        try:
            response = requests.post(api_url, headers=headers, json=payload)
            if response.status_code != 200:
                logger.error(f'Failed to upsert alarm. Status code: {response.status_code}')
        except requests.exceptions.RequestException as e:
            logger.error(f'Error during API call: {e}')

    def get_last_alarm(self, examId) :
        for alarm in self.last_alarm_list:
            if alarm.get('exam_id') == examId :
                return alarm
        return None

    def remove_last_alarm(self, id) :
        for alarm in self.last_alarm_list:
            if alarm.get('id') == id :
                self.last_alarm_list.remove(alarm)

    def upsert_alarm_rule(self, payload):
        api_url = 'https://iomt.karina-huinno.tk/webhook/alarm_rule_changed'
        headers = {
            'Content-Type': 'application/json',
        }

        try:
            response = requests.post(api_url, headers=headers, json=payload)
            if response.status_code != 200:
                logger.error(f'Failed to upsert alarm rule. Status code: {response.status_code}')
            self.get_alarm_rule()
            print(self.alarm_rule)
        except requests.exceptions.RequestException as e:
            logger.error(f'Error during API call: {e}')


    def get_exam_rule(self, serial_number) :
        for rule in self.alarm_rule:
            if rule.get('serial_number') == serial_number:
                return rule
        return None

    def get_exam_info(self, serial_number) :
        for exam_info in self.exam_serial:
            if exam_info.get('serial_number') == serial_number:
                return exam_info
        return None

    def check_alarm(self, serial_number, sensor_type, value):
        exam_info = self.get_exam_info(serial_number)
        if exam_info == None :
            return
        rule = self.get_exam_rule(serial_number) if self.get_exam_rule(serial_number) is not None else self.get_exam_rule(None)
        condition = json.loads(rule["condition"])

        if sensor_type == "hr":
            self.check_hr(condition["hrLimit"], int(value), exam_info["examination_id"], exam_info["ward_id"], exam_info["hospital_id"])

    def check_hr(self, hr_alarm_limit, value, exam_id, ward_id, hospital_id):
        if hr_alarm_limit["RED"]["HIGH"] is not None and value > hr_alarm_limit["RED"]["HIGH"]:
            self.send_mqtt_alarm(AlarmStatus.UNACK.value, exam_id, "HR HIGH", AlarmSeverity.CRITICAL.value, f"{value} > {hr_alarm_limit['RED']['HIGH']}", value, ward_id, hospital_id)
        elif hr_alarm_limit["RED"]["LOW"] is not None and value < hr_alarm_limit["RED"]["LOW"]:
            self.send_mqtt_alarm(AlarmStatus.UNACK.value, exam_id, "HR LOW", AlarmSeverity.CRITICAL.value, f"{value} < {hr_alarm_limit['RED']['LOW']}", value, ward_id, hospital_id)
        elif hr_alarm_limit["YELLOW"]["HIGH"] is not None and value > hr_alarm_limit["YELLOW"]["HIGH"]:
            self.send_mqtt_alarm(AlarmStatus.UNACK.value, exam_id, "HR HIGH", AlarmSeverity.MAJOR.value,f"{value} > {hr_alarm_limit['YELLOW']['HIGH']}", value, ward_id, hospital_id)
        elif hr_alarm_limit["YELLOW"]["LOW"] is not None and value < hr_alarm_limit["YELLOW"]["LOW"]:
            self.send_mqtt_alarm(AlarmStatus.UNACK.value, exam_id, "HR LOW", AlarmSeverity.MAJOR.value, f"{value} < {hr_alarm_limit['YELLOW']['LOW']}", value, ward_id, hospital_id)
        else :
            last_alarm = self.get_last_alarm(exam_id)
            if last_alarm != None :
                last_alarm['status'] = AlarmStatus.CLEARED.value
                self.client.pub(f"alarms/{hospital_id}/{ward_id}/{exam_id}", json.dumps(last_alarm))
                self.remove_last_alarm(last_alarm.get('id'))
            return

    def send_mqtt_alarm(self, status, examId, type, severity, limits, value, ward_id=1, hospital_id=123) :
        last_alarm = self.get_last_alarm(examId)
        if last_alarm == None :
            generated_uuid = uuid.uuid4()
            uuid_string = str(generated_uuid)
            payload = {
                "id" : uuid_string,
                "status": status,
                "exam_id": examId,
                "type": type,
                "severity": severity,
                "originator":"n8n",
                "limits":limits,
                "sender_id":"n8n_workflow",
                "pmc_volume":5,
                "pm_volume":0,
                "signalType":"HR",
                "detail": {
                    "hrValue":value,
                    "spO2Value":"--",
                    "tempValue":"--",
                    "nbpData": {
                        "systolic":"--",
                        "diastolic":"--",
                        "meanArterialPressure":"--",
                        "timestamp":"1970-01-01T00:00:00.000Z"}
                }
            }
            self.last_alarm_list.append(payload)
            self.client.pub(f"alarms/{hospital_id}/{ward_id}/{examId}", json.dumps(payload))
        elif last_alarm and last_alarm.get('type') == type and last_alarm.get('severity') == severity :
            return
        else :
            last_alarm['status'] = AlarmStatus.CLEARED.value
            self.client.pub(f"alarms/{hospital_id}/{ward_id}/{examId}", json.dumps(last_alarm))
            self.remove_last_alarm(last_alarm.get('id'))

            generated_uuid = uuid.uuid4()
            uuid_string = str(generated_uuid)
            payload = {
                "id" : uuid_string,
                "status": status,
                "exam_id": examId,
                "type": type,
                "severity": severity,
                "originator":"n8n",
                "limits":limits,
                "sender_id":"n8n_workflow",
                "pmc_volume":5,
                "pm_volume":0,
                "signalType":"HR",
                "detail": {
                    "hrValue":value,
                    "spO2Value":"--",
                    "tempValue":"--",
                    "nbpData": {
                        "systolic":"--",
                        "diastolic":"--",
                        "meanArterialPressure":"--",
                        "timestamp":"1970-01-01T00:00:00.000Z"}
                }
            }
            self.last_alarm_list.append(payload)
            self.client.pub(f"alarms/{hospital_id}/{ward_id}/{examId}", json.dumps(payload))

