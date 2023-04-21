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

from re import search

from simplejson import dumps

from thingsboard_gateway.gateway.constants import SEND_ON_CHANGE_PARAMETER
from thingsboard_gateway.connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter, log
from thingsboard_gateway.gateway.statistics_service import StatisticsService
from thingsboard_gateway.connectors.mqtt.alarm_manager import AlarmManager


class JsonMqttUplinkConverter(MqttUplinkConverter):
    def __init__(self, config):
        self.__config = config.get('converter')
        self.__send_data_on_change = self.__config.get(SEND_ON_CHANGE_PARAMETER)
        self.__alarm_manager = AlarmManager()

    @property
    def config(self):
        return self.__config

    @StatisticsService.CollectStatistics(start_stat_type='receivedBytesFromDevices',
                                         end_stat_type='convertedBytesFromDevice')
    def convert(self, topic, data):
        log.info('JsonMqttUplinkConverter convert')
        if isinstance(data, list):
            # topic: 'noti/alarm_rules',
            # {'alarm_rule_id': '1b79a578-d82b-11ed-a7d6-0a1ffb605237',
            # 'name': 'default', 'priority': 100, 'condition': '{"hrLimit":{"RED":{"HIGH":150,"LOW":40},"YELLOW":{"HIGH":120,"LOW":50}},"spO2Limit":{"RED":{"HIGH":null,"LOW":81},"YELLOW":{"HIGH":100,"LOW":90}},"btLimit":{"RED":{"HIGH":null,"LOW":null},"YELLOW":{"HIGH":39,"LOW":36}},"nbpSLimit":{"RED":{"HIGH":null,"LOW":null},"YELLOW":{"HIGH":160,"LOW":90}},"nbpDLimit":{"RED":{"HIGH":null,"LOW":null},"YELLOW":{"HIGH":90,"LOW":50}},"nbpMLimit":{"RED":{"HIGH":null,"LOW":null},"YELLOW":{"HIGH":110,"LOW":60}},"setting":{"sound":{"HR":true,"SpO2":true,"BT":true,"NBP":true,"level":3},"nbpListType":"Sys&Dia&Mean"}}', 'exam_ids': 'd952805a-d822-11ed-86ad-0a1ffb605237'})
            if topic == 'noti/alarm_rules':
                self.__alarm_manager.set_alarm_rules(data)
            if topic == 'noti/alarms':
                self.__alarm_manager.set_alarms(data)
            if topic == 'noti/exams':
                self.__alarm_manager.set_active_exam_sensors(data)
            # [{'alarm_id': 'a46c6382-bcb7-11ed-8c57-0a1ffb605350', 'type': 'High SpO2 Alarm', 'exam_id': '4d9c375e-b72f-11ed-906d-0a1ffb605237', 'sender_id': 'n8n_workflow', 'limits': '220>200', 'pmc_volume': 5, 'pm_volume': 1, 'hr': '90', 'spo2': '90', 'temp': '36.5', 'nbp_sys': '120', 'nbp_dia': '80', 'mean_arterial': '70', 'signal_type': 'SpO2'}]"
        else:
            if topic == 'noti/alarm':
                self.__alarm_manager.upsert_alarm(data)

        # log.info(self.__alarm_manager.get_alarms())
        # log.info(self.__alarm_manager.get_alarm_rules())
        # log.info(self.__alarm_manager.get_active_exam_sensors())
        return None

