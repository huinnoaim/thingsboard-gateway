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

import random
import string
from queue import Queue
from re import fullmatch
from threading import Thread
from time import sleep

import simplejson

from thingsboard_gateway.gateway.constants import SEND_ON_CHANGE_PARAMETER, DEFAULT_SEND_ON_CHANGE_VALUE, \
    ATTRIBUTES_PARAMETER, TELEMETRY_PARAMETER, SEND_ON_CHANGE_TTL_PARAMETER, DEFAULT_SEND_ON_CHANGE_INFINITE_TTL_VALUE
from thingsboard_gateway.gateway.constant_enums import Status
from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from paho.mqtt.client import Client, MQTTv31, MQTTv311, MQTTv5
import logging

log = logging.getLogger("connector")

MQTT_VERSIONS = {
    3: MQTTv31,
    4: MQTTv311,
    5: MQTTv5
}

RESULT_CODES_V3 = {
    1: "Connection rejected for unsupported protocol version",
    2: "Connection rejected for rejected client ID",
    3: "Connection rejected for unavailable server",
    4: "Connection rejected for damaged username or password",
    5: "Connection rejected for unauthorized",
}

RESULT_CODES_V5 = {
    4: "Disconnect with Will Message",
    16: "No matching subscribers",
    17: "No subscription existed",
    128: "Unspecified error",
    129: "Malformed Packet",
    130: "Protocol Error",
    131: "Implementation specific error",
    132: "Unsupported Protocol Version",
    133: "Client Identifier not valid",
    134: "Bad User Name or Password",
    135: "Not authorized",
    136: "Server unavailable",
    137: "Server busy",
    138: "Banned",
    139: "Server shutting down",
    140: "Bad authentication method",
    141: "Keep Alive timeout",
    142: "Session taken over",
    143: "Topic Filter invalid",
    144: "Topic Name invalid",
    145: "Packet Identifier in use",
    146: "Packet Identifier not found",
    147: "Receive Maximum exceeded",
    148: "Topic Alias invalid",
    149: "Packet too large",
    150: "Message rate too high",
    151: "Quota exceeded",
    152: "Administrative action",
    153: "Payload format invalid",
    154: "Retain not supported",
    155: "QoS not supported",
    156: "Use another server",
    157: "Server moved",
    158: "Shared Subscription not supported",
    159: "Connection rate exceeded",
    160: "Maximum connect time",
    161: "Subscription Identifiers not supported",
    162: "Wildcard Subscription not supported"
}


class MqttConnector(Connector, Thread):
    def __init__(self, gateway, config, connector_type):
        super().__init__()

        self.__gateway = gateway  # Reference to TB Gateway
        self._connector_type = connector_type  # Should be "mqtt"
        self.config = config  # mqtt.json contents

        self.__log = log
        self.__subscribes_sent = {}

        # Extract main sections from configuration ---------------------------------------------------------------------
        self.__broker = config.get('broker')
        self.__send_data_only_on_change = self.__broker.get(SEND_ON_CHANGE_PARAMETER, DEFAULT_SEND_ON_CHANGE_VALUE)
        self.__send_data_only_on_change_ttl = self.__broker.get(SEND_ON_CHANGE_TTL_PARAMETER,
                                                                DEFAULT_SEND_ON_CHANGE_INFINITE_TTL_VALUE)

        # for sendDataOnlyOnChange param
        self.__topic_content = {}

        self.__mapping = []
        self.__server_side_rpc = []
        self.__connect_requests = []
        self.__disconnect_requests = []
        self.__attribute_requests = []
        self.__attribute_updates = []

        mandatory_keys = {
            "mapping": ['topicFilter', 'converter']
        }

        # Mappings, i.e., telemetry/attributes-push handlers provided by user via configuration file
        self.load_handlers('mapping', mandatory_keys['mapping'], self.__mapping)

        # Setup topic substitution lists for each class of handlers ----------------------------------------------------
        self.__mapping_sub_topics = {}

        # Set up external MQTT broker connection -----------------------------------------------------------------------
        client_id = self.__broker.get("clientId", ''.join(random.choice(string.ascii_lowercase) for _ in range(23)))

        self._mqtt_version = self.__broker.get('version', 5)
        try:
            self._client = Client(client_id, protocol=MQTT_VERSIONS[self._mqtt_version])
        except KeyError:
            self.__log.error('Unknown MQTT version. Starting up on version 5...')
            self._client = Client(client_id, protocol=MQTTv5)
            self._mqtt_version = 5

        self.setName(config.get("name", self.__broker.get(
            "name",
            'Mqtt Broker ' + ''.join(random.choice(string.ascii_lowercase) for _ in range(5)))))

        # Set up external MQTT broker callbacks ------------------------------------------------------------------------
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_subscribe = self._on_subscribe
        self._client.on_disconnect = self._on_disconnect

        # Set up lifecycle flags ---------------------------------------------------------------------------------------
        self._connected = False
        self.__stopped = False
        self.daemon = True

        self.__msg_queue = Queue()
        self.__workers_thread_pool = []
        self.__max_msg_number_for_worker = config['broker'].get('maxMessageNumberPerWorker', 10)
        self.__max_number_of_workers = config['broker'].get('maxNumberOfWorkers', 100)

        self._on_message_queue = Queue()
        self._on_message_thread = Thread(name='On Message', target=self._process_on_message, daemon=True)
        print('mqtt_connector on_message thread starting')
        print(config)
        self._on_message_thread.start()

    def is_filtering_enable(self, device_name):
        return self.__send_data_only_on_change

    def get_config(self):
        return self.config

    def get_ttl_for_duplicates(self, device_name):
        return self.__send_data_only_on_change_ttl

    def load_handlers(self, handler_flavor, mandatory_keys, accepted_handlers_list):
        if handler_flavor not in self.config:
            self.__log.error("'%s' section missing from configuration", handler_flavor)
        else:
            for handler in self.config.get(handler_flavor):
                discard = False

                for key in mandatory_keys:
                    if key not in handler:
                        # Will report all missing fields to user before discarding the entry => no break here
                        discard = True
                        self.__log.error("Mandatory key '%s' missing from %s handler: %s",
                                         key, handler_flavor, simplejson.dumps(handler))
                    else:
                        self.__log.debug("Mandatory key '%s' found in %s handler: %s",
                                         key, handler_flavor, simplejson.dumps(handler))

                if discard:
                    self.__log.error("%s handler is missing some mandatory keys => rejected: %s",
                                     handler_flavor, simplejson.dumps(handler))
                else:
                    accepted_handlers_list.append(handler)
                    self.__log.debug("%s handler has all mandatory keys => accepted: %s",
                                     handler_flavor, simplejson.dumps(handler))

            self.__log.info("Number of accepted %s handlers: %d",
                            handler_flavor,
                            len(accepted_handlers_list))

            self.__log.info("Number of rejected %s handlers: %d",
                            handler_flavor,
                            len(self.config.get(handler_flavor)) - len(accepted_handlers_list))

    def is_connected(self):
        return self._connected

    def open(self):
        self.__stopped = False
        self.start()

    def run(self):
        try:
            self.__connect()
        except Exception as e:
            self.__log.exception(e)
            try:
                self.close()
            except Exception as e:
                self.__log.exception(e)

        while True:
            if self.__stopped:
                break
            elif not self._connected:
                self.__connect()
            self.__threads_manager()
            sleep(.2)

    def __connect(self):
        while not self._connected and not self.__stopped:
            try:
                self._client.connect(self.__broker['host'],
                                     self.__broker.get('port', 1883))
                self._client.loop_start()
                if not self._connected:
                    sleep(1)
            except ConnectionRefusedError as e:
                self.__log.error(e)
                sleep(10)

    def close(self):
        self.__stopped = True
        try:
            self._client.disconnect()
        except Exception as e:
            log.exception(e)
        self._client.loop_stop()
        self.__log.info('%s has been stopped.', self.get_name())

    def get_name(self):
        return self.name

    def __subscribe(self, topic, qos):
        message = self._client.subscribe(topic, qos)
        try:
            self.__subscribes_sent[message[1]] = topic
        except Exception as e:
            self.__log.exception(e)

    def _on_connect(self, client, userdata, flags, result_code, *extra_params):
        if result_code == 0:
            self._connected = True
            self.__log.info('%s connected to %s:%s - successfully.',
                            self.get_name(),
                            self.__broker["host"],
                            self.__broker.get("port", "1883"))

            self.__log.debug("Client %s, userdata %s, flags %s, extra_params %s",
                             str(client),
                             str(userdata),
                             str(flags),
                             extra_params)

            self.__mapping_sub_topics = {}

            # Setup data upload requests handling ----------------------------------------------------------------------
            for mapping in self.__mapping:
                try:
                    # Load converter for this mapping entry ------------------------------------------------------------
                    # mappings are guaranteed to have topicFilter and converter fields. See __init__
                    default_converters = {
                        "json": "JsonMqttUplinkConverter",
                        "bytes": "BytesMqttUplinkConverter"
                    }

                    # Get converter class from "extension" parameter or default converter
                    converter_class_name = mapping["converter"].get("extension",
                                                                    default_converters.get(
                                                                        mapping['converter'].get('type')))
                    if not converter_class_name:
                        self.__log.error('Converter type or extension class should be configured!')
                        continue

                    # Find and load required class
                    module = TBModuleLoader.import_module(self._connector_type, converter_class_name)
                    if module:
                        self.__log.debug('Converter %s for topic %s - found!', converter_class_name,
                                         mapping["topicFilter"])
                        converter = module(mapping)
                    else:
                        self.__log.error("Cannot find converter for %s topic", mapping["topicFilter"])
                        continue

                    # Setup regexp topic acceptance list ---------------------------------------------------------------
                    # Check if topic is shared subscription type
                    # (an exception is aws topics that do not support shared subscription)
                    regex_topic = mapping["topicFilter"]
                    if not regex_topic.startswith('$aws') and regex_topic.startswith('$'):
                        regex_topic = '/'.join(regex_topic.split('/')[2:])
                    else:
                        regex_topic = TBUtility.topic_to_regex(regex_topic)

                    # There may be more than one converter per topic, so I'm using vectors
                    if not self.__mapping_sub_topics.get(regex_topic):
                        self.__mapping_sub_topics[regex_topic] = []

                    self.__mapping_sub_topics[regex_topic].append(converter)

                    # Subscribe to appropriate topic -------------------------------------------------------------------
                    self.__subscribe(mapping["topicFilter"], mapping.get("subscriptionQos", 1))

                    self.__log.info('Connector "%s" subscribe to %s',
                                    self.get_name(),
                                    TBUtility.regex_to_topic(regex_topic))

                except Exception as e:
                    self.__log.exception(e)

        else:
            result_codes = RESULT_CODES_V5 if self._mqtt_version == 5 else RESULT_CODES_V3
            if result_code in result_codes:
                self.__log.error("%s connection FAIL with error %s %s!", self.get_name(), result_code,
                                 result_codes[result_code])
            else:
                self.__log.error("%s connection FAIL with unknown error!", self.get_name())

    def _on_disconnect(self, *args):
        self._connected = False
        self.__log.debug('"%s" was disconnected. %s', self.get_name(), str(args))

    def _on_log(self, *args):
        self.__log.debug(args)

    def _on_subscribe(self, _, __, mid, granted_qos, *args):
        log.info(args)
        try:
            if granted_qos[0] == 128:
                self.__log.error('"%s" subscription failed to topic %s subscription message id = %i',
                                 self.get_name(),
                                 self.__subscribes_sent.get(mid), mid)
            else:
                self.__log.info('"%s" subscription success to topic %s, subscription message id = %i',
                                self.get_name(),
                                self.__subscribes_sent.get(mid), mid)
        except Exception as e:
            self.__log.exception(e)

        # Success or not, remove this topic from the list of pending subscription requests
        if self.__subscribes_sent.get(mid) is not None:
            del self.__subscribes_sent[mid]

    def put_data_to_convert(self, converter, message, content) -> bool:
        if not self.__msg_queue.full():
            self.__msg_queue.put((converter.convert, message.topic, content), True, 100)
            self.__log.info('Data for converting is added to queue')
            return True
        return False

    def _save_converted_msg(self, topic, data):
        if self.__gateway.send_to_storage(self.name, data) == Status.SUCCESS:
            self.__log.debug("Successfully converted message from topic %s", topic)

    def __threads_manager(self):
        worker_idx = len(self.__workers_thread_pool)
        if len(self.__workers_thread_pool) == 0:
            worker = MqttConnector.ConverterWorker(f"MqttConnector ConverterWorker-{worker_idx}",
                                                   self.__msg_queue,
                                                   self._save_converted_msg,
                                                   self._client)
            self.__workers_thread_pool.append(worker)
            worker.start()

        number_of_needed_threads = round(self.__msg_queue.qsize() / self.__max_msg_number_for_worker, 0)
        threads_count = len(self.__workers_thread_pool)
        if number_of_needed_threads > threads_count < self.__max_number_of_workers:
            thread = MqttConnector.ConverterWorker(
                f"MqttConnector ConverterWorker-{worker_idx}-" + ''.join(random.choice(string.ascii_lowercase) for _ in range(5)), self.__msg_queue,
                self._save_converted_msg, self._client)
            self.__workers_thread_pool.append(thread)
            thread.start()
        elif number_of_needed_threads < threads_count and threads_count > 1:
            worker: MqttConnector.ConverterWorker = self.__workers_thread_pool[-1]
            if not worker.in_progress:
                worker.stopped = True
                self.__workers_thread_pool.remove(worker)

    def _on_message(self, client, userdata, message):
        # print(message.payload)
        self._on_message_queue.put((client, userdata, message))

    def _process_on_message(self):
        while not self.__stopped:
            if not self._on_message_queue.empty():
                client, userdata, message = self._on_message_queue.get()

                content = TBUtility.decode(message)

                # Check if message topic exists in mappings "i.e., I'm posting telemetry/attributes" -------------------
                topic_handlers = [regex for regex in self.__mapping_sub_topics if fullmatch(regex, message.topic)]

                if topic_handlers:
                    request_handled = False

                    for topic in topic_handlers:
                        available_converters = self.__mapping_sub_topics[topic]
                        for converter in available_converters:
                            try:
                                # check if data is equal
                                if converter.config.get('sendDataOnlyOnChange', False) and self.__topic_content.get(message.topic) == content:
                                    request_handled = True
                                    continue

                                self.__topic_content[message.topic] = content
                                request_handled = self.put_data_to_convert(converter, message, content)
                            except Exception as e:
                                log.exception(e)

                    if not request_handled:
                        self.__log.error('Cannot find converter for the topic:"%s"! Client: %s, User data: %s',
                                         message.topic,
                                         str(client),
                                         str(userdata))

                    # Note: if I'm in this branch, this was for sure a telemetry/attribute push message
                    # => Execution must end here both in case of failure and success
                    continue

                self.__log.debug("Received message to topic \"%s\" with unknown interpreter data: \n\n\"%s\"",
                                 message.topic,
                                 content)

            sleep(.2)

    def notify_attribute(self, incoming_data, attribute_name, topic_expression, value_expression, retain):
        if incoming_data.get("device") is None or incoming_data.get("value", incoming_data.get('values')) is None:
            return

        device_name = incoming_data.get("device")
        attribute_values = incoming_data.get('value') or incoming_data.get('values')

        topic = topic_expression \
            .replace("${deviceName}", str(device_name)) \
            .replace("${attributeKey}", str(attribute_name))

        if len(attribute_name) <= 1:
            data = value_expression.replace("${attributeKey}", str(attribute_name[0])) \
                .replace("${attributeValue}", str(attribute_values))
        else:
            data = simplejson.dumps(attribute_values)

        self._client.publish(topic, data, retain=retain).wait_for_publish()

    def _publish(self, request_topic, data_to_send, retain):
        # Publish ts kv
        self._client.publish(request_topic, data_to_send, retain)\
                    .wait_for_publish()
        log.info('Thingsboard Data is published')

    class ConverterWorker(Thread):
        def __init__(self, name, incoming_queue, send_result, mqtt_client: Client):
            super().__init__()
            self.stopped = False
            self.setName(name)
            self.setDaemon(True)
            self.__msg_queue = incoming_queue
            self.in_progress = False
            self.__send_result = send_result
            self._client = mqtt_client

        def run(self):
            while not self.stopped:
                if not self.__msg_queue.empty():
                    self.in_progress = True
                    convert_function, config, incoming_data = self.__msg_queue.get(True, 100)
                    converted_data = convert_function(config, incoming_data)

                    if converted_data and (converted_data.get(ATTRIBUTES_PARAMETER) or
                                           converted_data.get(TELEMETRY_PARAMETER)):
                        self.__send_result(config, converted_data)

                    if converted_data and converted_data.get('alarm'):
                        alarm = converted_data.get('alarm')
                        if alarm.get('hospital_id') and alarm.get('ward_id') and alarm.get('exam_id'):
                            log.info('trigger ALARM')
                            # serial_number, hospital_id, ward_id, exam_id
                            topic_name = f'alarms/{alarm.hospital_id}/{alarm.ward_id}/{alarm.exam_id}'
                            self._client.publish(topic_name, converted_data)

                    self.in_progress = False
                else:
                    sleep(.2)
