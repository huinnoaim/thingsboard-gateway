# Alarm Rule Engine and Manager

import logging
import json
import time
import random
import threading
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.connectors.connector import Connector, log


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

    def run(self):
        while True:
            time.sleep(1)

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

    # todo: clear alarm 시 alarm 삭제
    def upsert_alarm(self, new_alarm):
        log.info('upsert_alarm')
        log.info(new_alarm)
        # {'alarm_id': 'a46c6382-bcb7-11ed-8c58-0a1ffb605351', 'type': 'Low SpO2 Alarm', 'exam_id': '4d9c375e-b72f-11ed-906d-0a1ffb605238', 'sender_id': 'n8n_workflow', 'limits': '180>160', 'pmc_volume': 6, 'pm_volume': 2, 'hr': '80', 'spo2': '80', 'temp': '36.0', 'nbp_sys': '110', 'nbp_dia': '70', 'mean_arterial': '65', 'signal_type': 'SpO2'}

        # Check if the element already exists in the array
        existing_element = next((elem for elem in self.__alarms if elem['alarm_id'] == new_alarm['alarm_id']), None)

        if existing_element is not None:
            # Update the existing element with the new values
            existing_element.update(new_alarm)
        else:
            # Add the new element to the array
            self.__alarms.append(new_alarm)

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

        return {
            'hrAlarmType': alarm_type,
            'hrAlarmContent': content,
            'hrLimitContent': limit_content,
        }

    def find_alarms_if_met_condition(self, dict_result):
        log.info('check_alarm_condition')
        serial_number = dict_result['deviceName']

        # find exam_id from deviceName
        # check prev alarm exist
        # get rules per exam_id
        # check rule
        # trigger alarm

        existing_exam = next((elem for elem in self.__active_exam_sensors if elem['serial_number'] == serial_number), None)
        if existing_exam is None:
            return None

        log.info(existing_exam)

        matching_rules = [elem for elem in self.__alarm_rules if elem['exam_ids'] in ['*', existing_exam['exam_id']]]
        log.info(matching_rules)

        # loop per matching rules
        hr = dict_result['telemetry'][0]['values']['hr']
        # limits, pmc_volume, pm_volume, hr, spo2, temp, nbp_sys, nbp_dia, mean_arterial, signal_type
        alarms = [elem for elem in matching_rules if self.check_hr_alarm_limit(elem, hr) is not None]

        return alarms
