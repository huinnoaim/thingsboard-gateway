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
from thingsboard_gateway.connectors.mqtt.http_manager import HttpManager
import logging

log = logging.getLogger("converter")


class JsonMqttUplinkConverter(MqttUplinkConverter):
    def __init__(self, config, ai_queue, trigger_queue):
        self.__config = config.get('converter')
        self.__send_data_on_change = self.__config.get(SEND_ON_CHANGE_PARAMETER)
        self.__alarm_manager = AlarmManager()
        self.__http_manager = HttpManager(ai_queue, trigger_queue)
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
                new_active_exam_sensor = self.__alarm_manager.set_active_exam_sensors(data)
                self.__http_manager.trigger_http('exam_sensor_changed', new_active_exam_sensor)
        else:
            if topic.startswith('alarms'):
                print('start handle_alarm')
                new_alarm = self.__alarm_manager.handle_alarm(topic, data)
                if new_alarm:
                    self.__http_manager.trigger_http('alarms', new_alarm)

            if topic.startswith('noti/alarm-rules'):
                new_alarm_rule = self.__alarm_manager.upsert_alarm_rule(topic, data)
                if new_alarm_rule:
                    self.__http_manager.trigger_http('alarm_rule_changed', new_alarm_rule)
            if topic.startswith('noti/alarm'):
                new_alarm = self.__alarm_manager.upsert_alarm(topic, data)
                if new_alarm:
                    self.__http_manager.trigger_http('alarm_changed', new_alarm)
            if topic.startswith('noti/exam'):
                new_active_exam_sensor = self.__alarm_manager.upsert_active_exam_sensor(topic, data)
                if new_active_exam_sensor:
                    self.__http_manager.trigger_http('exam_sensor_changed', new_active_exam_sensor)
        end_time = timer()
        log.debug('<<mqtt json converter elapsed time>>: ' + str(end_time - start_time))
        return None
