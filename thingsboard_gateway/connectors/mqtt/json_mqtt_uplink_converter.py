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
from timeit import default_timer as timer
from thingsboard_gateway.gateway.constants import SEND_ON_CHANGE_PARAMETER
from thingsboard_gateway.connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter
from thingsboard_gateway.connectors.mqtt.alarm_manager import AlarmManager
import logging

log = logging.getLogger("hr_detector")


class JsonMqttUplinkConverter(MqttUplinkConverter):
    def __init__(self, config):
        self.__config = config.get('converter')
        self.__send_data_on_change = self.__config.get(SEND_ON_CHANGE_PARAMETER)
        self.__alarm_manager = AlarmManager()
        log.debug('JsonMqttUplinkConverter init')

    @property
    def config(self):
        return self.__config

    def convert(self, topic, data):
        log.debug('JsonMqttUplinkConverter convert:' + topic)
        start_time = timer()
        if isinstance(data, list):
            if topic.startswith('noti/alarm-rules'):
                self.__alarm_manager.set_alarm_rules(data)
            if topic.startswith('noti/alarms'):
                self.__alarm_manager.set_alarms(data)
            if topic.startswith('noti/exams'):
                self.__alarm_manager.set_active_exam_sensors(data)
        else:
            if topic.startswith('alarms'):
                print('start handle_alarm')
                self.__alarm_manager.handle_alarm(topic, data)
            if topic.startswith('noti/alarm-rules'):
                self.__alarm_manager.upsert_alarm_rule(topic, data)
            if topic.startswith('noti/alarm'):
                self.__alarm_manager.upsert_alarm(topic, data)
            if topic.startswith('noti/exam'):
                self.__alarm_manager.upsert_active_exam_sensor(topic, data)
        end_time = timer()
        log.debug('<<mqtt json converter elapsed time>>: ' + str(end_time - start_time))
        return None
