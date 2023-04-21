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
        self.__alarm_rules = []
        self.__alarms = []

    def run(self):
        while True:
            time.sleep(1)

    def get_alarm_rules(self):
        return self.__alarm_rules

    def set_alarm_rules(self, new_list):
        self.__alarm_rules = new_list

    def get_alarms(self):
        return self.__alarms

    def set_alarms(self, new_list):
        self.__alarms = new_list

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
            
