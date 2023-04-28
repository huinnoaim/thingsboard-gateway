# Alarm Rule Engine and Manager

import re
import random
import asyncio
import aiohttp
from thingsboard_gateway.connectors.connector import Connector, log

TRIGGER_BASE_URL = "https://iomt.karina-huinno.tk/iomt-api/"
regex_uuid = re.compile("^[0-9a-f]{8}-[0-9a-f]{4}-[4-9a-f]{4}-[89ab-cd ef]{3}-[0-9a-f]{12}$")

async def trigger_http(url_path, body):
    async with aiohttp.ClientSession() as session:
        payload = body
        headers = {
            'Content-Type': 'application/json'
        }
        async with session.post(TRIGGER_BASE_URL + url_path, headers=headers, data=payload) as response:
            print("Status:", response.status)
            print("Content-type:", response.headers['content-type'])
            html = await response.text()
            print("Body:", html[:30], "...")

class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls) \
                .__call__(*args, **kwargs)
        return cls._instances[cls]


class AlarmManager(metaclass=Singleton):
    def __init__(self):
        self.id = random.randint(0, 10000)
        print(f"Instance ID: {self.id}")
        self.__alarm_rules = [] # alarm_rule_id, exam_ids
        self.__alarms = []
        self.__active_exam_sensors = []  # exam_id, thingboard_id

    def handle_alarm(self, topic, new_alarm):
        match = re.match(r'^alarms/(\d+)/([^/]+)/([^/]+)$', topic)
        if match is None:
            log.error('Invalid alarm URL: {}'.format(topic))
            return self

        hospital_id, ward_id, exam_id = match.groups()
        if hospital_id is None or ward_id is None or exam_id is None:
            log.error('invalid topic requested:' + topic)
            return self

        alarm_id = new_alarm.get('id')
        if not regex_uuid.match(alarm_id):
            log.warn('invalid alarm id: ' + alarm_id)
            return self
        exam_id = new_alarm.get('exam_id')
        if not regex_uuid.match(exam_id):
            log.warn('invalid exam_id: ' + exam_id)
            return self
        originator = new_alarm.get('originator')
        if originator != 'pm':
            log.debug('ignore: originator is not pm' + originator)
            return self
        status = new_alarm.get('status')
        log.debug('status:' + status)
        if status == 'ACTIVE_ACK':
            if new_alarm.get('checkFromPmc'):
                log.debug('ignore checked from PMC')
                return self
        # if status == 'ACTIVE_UNACK':
        # if status == 'CLEARED_ACK':
        trigger_http('alarms/'+hospital_id+'/'+ward_id+'/'+exam_id, new_alarm)

    def get_alarm_rules(self):
        return self.__alarm_rules

    def set_alarm_rules(self, new_list):
        self.__alarm_rules = new_list

    def get_active_exam_sensors(self):
        return self.__active_exam_sensors

    def set_active_exam_sensors(self, new_list):
        self.__active_exam_sensors = new_list

    def get_alarms(self):
        return self.__alarms

    def set_alarms(self, new_list):
        self.__alarms = new_list

    def upsert_alarm_rule(self, topic, new_alarm_rule):
        log.info('upsert_alarm_rule')
        log.info(new_alarm_rule)

        match = re.match(r'^noti/alarm_rules/(\d+)/([^/]+)/([^/]+)$', topic)
        if match is None:
            log.error('Invalid alarm URL: {}'.format(topic))
            return self

        hospital_id, ward_id, exam_id = match.groups()
        if hospital_id is None or ward_id is None or exam_id is None:
            log.error('invalid topic requested:' + topic)
            return self

        # Check if the element already exists in the array
        existing_element = next((elem for elem in self.__alarm_rules if elem['id'] == new_alarm_rule['id']), None)

        if existing_element is not None:
            existing_element.update(new_alarm_rule)
        else:
            self.__alarms.append(new_alarm_rule)
        trigger_http('alarm_rule_changed/'+hospital_id+'/'+ward_id+'/'+exam_id, new_alarm_rule)

    def upsert_active_exam_sensor(self, topic, new_active_exam_sensor):
        log.info('upsert_active_exam_sensor')
        log.info(new_active_exam_sensor)

        match = re.match(r'^noti/exam_sensors/(\d+)/([^/]+)/([^/]+)$', topic)
        if match is None:
            log.error('Invalid alarm URL: {}'.format(topic))
            return self

        hospital_id, ward_id, exam_id = match.groups()
        if hospital_id is None or ward_id is None or exam_id is None:
            log.error('invalid topic requested:' + topic)
            return self

        # Check if the element already exists in the array
        existing_element = next((elem for elem in self.__active_exam_sensors if elem['serial_number'] == new_active_exam_sensor['serial_number']), None)

        if existing_element is not None:
            existing_element.update(new_active_exam_sensor)
        else:
            self.__alarms.append(new_active_exam_sensor)
        trigger_http('exam_sensor_changed/'+hospital_id+'/'+ward_id+'/'+exam_id, new_active_exam_sensor)

    def upsert_alarm(self, topic, new_alarm):
        log.info('upsert_alarm')
        log.info(new_alarm)
        # {'alarm_id': 'a46c6382-bcb7-11ed-8c58-0a1ffb605351', 'type': 'Low SpO2 Alarm',
        # 'exam_id': '4d9c375e-b72f-11ed-906d-0a1ffb605238', 'sender_id': 'n8n_workflow',
        # 'limits': '180>160', 'pmc_volume': 6, 'pm_volume': 2, 'hr': '80', 'spo2': '80', 'temp': '36.0',
        # 'nbp_sys': '110', 'nbp_dia': '70', 'mean_arterial': '65', 'signal_type': 'SpO2'}
        match = re.match(r'^noti/alarm/(\d+)/([^/]+)/([^/]+)$', topic)
        if match is None:
            log.error('Invalid alarm URL: {}'.format(topic))
            return self

        hospital_id, ward_id, exam_id = match.groups()
        if hospital_id is None or ward_id is None or exam_id is None:
            log.error('invalid topic requested:' + topic)
            return self

        # Check if the element already exists in the array
        existing_element = next((elem for elem in self.__alarms if elem['alarm_id'] == new_alarm['alarm_id']), None)

        if existing_element is not None:
            existing_element.update(new_alarm)
        else:
            self.__alarms.append(new_alarm)
        trigger_http('alarm_changed/'+hospital_id+'/'+ward_id+'/'+exam_id, new_alarm)

    def check_hr_alarm_limit(self, hr_alarm_limit, hr_value):
        # hrLimit =  { \"RED\": { \"HIGH\": 150, \"LOW\": 40 }, \"YELLOW\": { \"HIGH\": 120, \"LOW\": 50 }
        alarm_type = None
        content = None
        limit_content = ''
        if hr_value == '——':
            alarm_type = 'AlarmConst.TYPE.INOP'
            content = 'AlarmConst.INOP.SENSOR.ECG_DEVICE.LEAD_OFF'
        elif hr_alarm_limit['RED']['HIGH'] < hr_value:
            alarm_type = 'AlarmConst.TYPE.RED'
            content = 'AlarmConst.RED.HR_HIGH'
            limit_content = f"{hr_value} > {hr_alarm_limit['RED']['HIGH']}"
        elif hr_alarm_limit['RED']['LOW'] > hr_value:
            alarm_type = 'AlarmConst.TYPE.RED'
            content = 'AlarmConst.RED.HR_LOW'
            limit_content = f"{hr_value} < {hr_alarm_limit['RED']['LOW']}"
        elif hr_alarm_limit['YELLOW']['HIGH'] < hr_value:
            alarm_type = 'AlarmConst.TYPE.YELLOW'
            content = 'AlarmConst.YELLOW.HR_HIGH'
            limit_content = f"{hr_value} > {hr_alarm_limit['YELLOW']['HIGH']}"
        elif hr_alarm_limit['YELLOW']['LOW'] > hr_value:
            alarm_type = 'AlarmConst.TYPE.YELLOW'
            content = 'AlarmConst.YELLOW.HR_LOW'
            limit_content = f"{hr_value} < {hr_alarm_limit['YELLOW']['LOW']}"

        result = {
            'hrAlarmType': alarm_type,
            'hrAlarmContent': content,
            'hrLimitContent': limit_content,
        }
        log.info(result)
        return result

    def find_alarms_if_met_condition(self, dict_result):
        log.info('check_alarm_condition')

        telemetry_data = dict_result.get('telemetry', [{}])
        hr = telemetry_data[0].get('values', {}).get('hr', None)
        if hr is None:
            return None

        serial_number = dict_result['deviceName']

        existing_exam = next((elem for elem in self.__active_exam_sensors if elem['serial_number'] == serial_number), None)
        if existing_exam is None:
            log.debug('no existing_exam')
            return None

        log.debug(existing_exam)

        matching_rules = [elem for elem in self.__alarm_rules if elem['exam_ids'] in ['*', existing_exam['exam_id']]]
        log.info(matching_rules)

        # loop per matching rules
        # limits, pmc_volume, pm_volume, hr, spo2, temp, nbp_sys, nbp_dia, mean_arterial, signal_type
        alarm = next((elem for elem in matching_rules if self.check_hr_alarm_limit(elem, hr) is not None), None)
        if alarm is not None:
            alarm['hospital_id'] = existing_exam['hospital_id']
            alarm['ward_id'] = existing_exam['ward_id']
            alarm['exam_id'] = existing_exam['exam_id']

        return alarm
