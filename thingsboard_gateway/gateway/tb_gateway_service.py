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

import logging
import logging.config
import logging.handlers
import multiprocessing.managers
import time
from signal import signal, SIGINT
from os import listdir, path, stat, system, environ
from queue import SimpleQueue
from random import choice
from string import ascii_lowercase, hexdigits
from sys import getsizeof
from threading import RLock, Thread
from time import sleep, time

from simplejson import JSONDecodeError, dumps, load, loads
from yaml import safe_load

from thingsboard_gateway.gateway.constant_enums import DeviceActions, Status
from thingsboard_gateway.gateway.constants import CONNECTED_DEVICES_FILENAME, CONNECTOR_PARAMETER, \
    PERSISTENT_GRPC_CONNECTORS_KEY_FILENAME
from thingsboard_gateway.gateway.device_filter import DeviceFilter
from thingsboard_gateway.gateway.duplicate_detector import DuplicateDetector
from thingsboard_gateway.gateway.tb_client import TBClient
from thingsboard_gateway.storage.memory.memory_event_storage import MemoryEventStorage
from thingsboard_gateway.tb_utility.tb_logger import TBLoggerHandler
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader

log = logging.getLogger('service')
main_handler = logging.handlers.MemoryHandler(-1)

DEFAULT_CONNECTORS = {
    "mqtt": "MqttConnector"
}

DEFAULT_DEVICE_FILTER = {
    'enable': False
}

SECURITY_VAR = ('accessToken', 'caCert', 'privateKey', 'cert')


def load_file(path_to_file):
    with open(path_to_file, 'r') as target_file:
        content = load(target_file)
        return content


def get_env_variables():
    env_variables = {
        'host': environ.get('host'),
        'port': int(environ.get('port')) if environ.get('port') else None,
        'accessToken': environ.get('accessToken'),
        'caCert': environ.get('caCert'),
        'privateKey': environ.get('privateKey'),
        'cert': environ.get('cert')
    }

    converted_env_variables = {}

    for (key, value) in env_variables.items():
        if value is not None:
            if key in SECURITY_VAR:
                if not converted_env_variables.get('security'):
                    converted_env_variables['security'] = {}

                converted_env_variables['security'][key] = value
            else:
                converted_env_variables[key] = value

    return converted_env_variables


class GatewayManager(multiprocessing.managers.BaseManager):
    def __init__(self, address=None, authkey=b''):
        super().__init__(address=address, authkey=authkey)
        self.gateway = None

    def has_gateway(self):
        return self.gateway is not None

    def add_gateway(self, gateway):
        self.gateway = gateway


class TBGatewayService:
    EXPOSED_GETTERS = [
        'ping',
        'get_status',
        'get_storage_name',
        'get_storage_events_count',
        'get_available_connectors',
        'get_connector_status',
        'get_connector_config'
    ]

    def __init__(self, config_file=None):
        signal(SIGINT, lambda _, __: self.__stop_gateway())

        self.stopped = False
        self.__lock = RLock()
        self.async_device_actions = {
            DeviceActions.CONNECT: self.add_device,
            DeviceActions.DISCONNECT: self.del_device
        }
        self.__async_device_actions_queue = SimpleQueue()
        self.__process_async_actions_thread = Thread(target=self.__process_async_device_actions,
                                                     name="Async device actions processing thread", daemon=True)
        if config_file is None:
            dirname = path.dirname(path.dirname(path.abspath(__file__)))
            config_file = dirname + '/config/tb_gateway.yaml'.replace('/', path.sep)
        with open(config_file) as general_config:
            self.__config = safe_load(general_config)

        # change main config if Gateway running with docker env variables
        self.__modify_main_config()

        self._config_dir = path.dirname(path.abspath(config_file)) + path.sep
        logging_error = None
        try:
            logging.config.fileConfig(self._config_dir + "logs.conf", disable_existing_loggers=False)
        except Exception as e:
            logging_error = e

        log.info("Gateway starting...")
        self.available_connectors = {}
        self.__connector_incoming_messages = {}
        self.__connected_devices = {}
        self.__renamed_devices = {}
        self.__saved_devices = {}
        self.__events = []
        self.name = ''.join(choice(ascii_lowercase) for _ in range(64))
        self.__rpc_register_queue = SimpleQueue()
        self.__rpc_requests_in_progress = {}
        self.tb_client = TBClient(self.__config["thingsboard"], self._config_dir)
        try:
            self.tb_client.disconnect()
        except Exception as e:
            log.exception(e)

        self.tb_client.connect()
        self.subscribe_to_required_topics()
        self.__subscribed_to_rpc_topics = True
        if logging_error is not None:
            self.tb_client.client.send_telemetry({"ts": time() * 1000, "values": {
                "LOGS": "Logging loading exception, logs.conf is wrong: %s" % (str(logging_error),)}})
            TBLoggerHandler.set_default_handler()
        self.counter = 0
        self.__rpc_reply_sent = False

        global main_handler
        self.main_handler = main_handler
        self.remote_handler = TBLoggerHandler(self)
        self.main_handler.setTarget(self.remote_handler)
        self._default_connectors = DEFAULT_CONNECTORS
        self.__converted_data_queue = SimpleQueue()
        self.__save_converted_data_thread = Thread(name="Save converted data", daemon=True,
                                                   target=self.__send_to_storage)
        self.__save_converted_data_thread.start()
        self._implemented_connectors = {}
        self._event_storage_types = {
            "memory": MemoryEventStorage
        }
        self._event_storage = self._event_storage_types[self.__config["storage"]["type"]](self.__config["storage"])
        self.connectors_configs = {}
        self.__request_config_after_connect = False
        self._load_connectors()
        self._connect_with_connectors()
        self.__load_persistent_devices()
        self._published_events = SimpleQueue()

        self.__min_pack_send_delay_ms = self.__config['thingsboard'].get('minPackSendDelayMS', 200)
        if self.__min_pack_send_delay_ms == 0:
            self.__min_pack_send_delay_ms = 10

        self.__min_pack_send_delay_ms = self.__min_pack_send_delay_ms / 1000.0
        self.__min_pack_size_to_send = self.__config['thingsboard'].get('minPackSizeToSend', 50)

        self._send_thread = Thread(target=self.__read_data_from_storage, daemon=True,
                                   name="Send data to Thingsboard Thread")
        self._send_thread.start()

        self.__device_filter_config = self.__config['thingsboard'].get('deviceFiltering', DEFAULT_DEVICE_FILTER)
        self.__device_filter = None
        if self.__device_filter_config['enable']:
            self.__device_filter = DeviceFilter(config_path=self._config_dir + self.__device_filter_config[
                'filterFile'] if self.__device_filter_config.get('filterFile') else None)

        self.__duplicate_detector = DuplicateDetector(self.available_connectors)

        log.info("Gateway started.")

        self._watchers_thread = Thread(target=self._watchers, name='Watchers', daemon=True)
        self._watchers_thread.start()

        if path.exists('/tmp/gateway'):
            try:
                # deleting old manager if it was closed incorrectly
                system('rm -rf /tmp/gateway')
            except OSError as e:
                log.exception(e)

        self.manager = GatewayManager(address='/tmp/gateway', authkey=b'gateway')
        GatewayManager.register('get_gateway', self.get_gateway, proxytype=None, exposed=self.EXPOSED_GETTERS,
                                create_method=False)
        self.server = self.manager.get_server()
        self.server.serve_forever()

    def get_gateway(self):
        if self.manager.has_gateway():
            return self.manager.gateway
        else:
            self.manager.add_gateway(self)
            self.manager.register('gateway', lambda: self, proxytype=None)

    def _watchers(self):
        try:
            while not self.stopped:
                if not self.tb_client.is_connected() and self.__subscribed_to_rpc_topics:
                    self.__subscribed_to_rpc_topics = False

                if self.tb_client.is_connected() and not self.__subscribed_to_rpc_topics:
                    for device in self.__saved_devices:
                        self.add_device(device, {"connector": self.__saved_devices[device]["connector"]},
                                        device_type=self.__saved_devices[device]["device_type"])
                    self.subscribe_to_required_topics()
                    self.__subscribed_to_rpc_topics = True

                if not self.tb_client.is_connected():
                    try:
                        sleep(0.2)
                    except Exception as e:
                        log.exception(e)
                        break

        except Exception as e:
            log.exception(e)
            self.__stop_gateway()
            self.__close_connectors()
            log.info("The gateway has been stopped.")
            self.tb_client.stop()

    def __modify_main_config(self):
        env_variables = get_env_variables()
        self.__config['thingsboard'] = {**self.__config['thingsboard'], **env_variables}

    def __close_connectors(self):
        for current_connector in self.available_connectors:
            try:
                self.available_connectors[current_connector].close()
                log.debug("Connector %s closed connection.", current_connector)
            except Exception as e:
                log.exception(e)

    def __stop_gateway(self):
        self.stopped = True
        log.info("Stopping...")

        self.__close_connectors()
        self._event_storage.stop()
        log.info("The gateway has been stopped.")
        self.tb_client.disconnect()
        self.tb_client.stop()
        if self.manager:
            self.manager.shutdown()

    def __process_deleted_gateway_devices(self, deleted_device_name: str):
        log.info("Received deleted gateway device notification: %s", deleted_device_name)
        if deleted_device_name in list(self.__renamed_devices.values()):
            first_device_name = TBUtility.get_dict_key_by_value(self.__renamed_devices, deleted_device_name)
            del self.__renamed_devices[first_device_name]
            deleted_device_name = first_device_name
            log.debug("Current renamed_devices dict: %s", self.__renamed_devices)
        if deleted_device_name in self.__connected_devices:
            del self.__connected_devices[deleted_device_name]
            log.debug("Device %s - was removed from __connected_devices", deleted_device_name)
        if deleted_device_name in self.__saved_devices:
            del self.__saved_devices[deleted_device_name]
            log.debug("Device %s - was removed from __saved_devices", deleted_device_name)
        self.__duplicate_detector.delete_device(deleted_device_name)
        self.__save_persistent_devices()
        self.__load_persistent_devices()

    def __process_renamed_gateway_devices(self, renamed_device: dict):
        if self.__config.get('handleDeviceRenaming', True):
            log.info("Received renamed gateway device notification: %s", renamed_device)
            old_device_name, new_device_name = renamed_device.popitem()
            if old_device_name in list(self.__renamed_devices.values()):
                device_name_key = TBUtility.get_dict_key_by_value(self.__renamed_devices, old_device_name)
            else:
                device_name_key = new_device_name
            self.__renamed_devices[device_name_key] = new_device_name
            self.__duplicate_detector.rename_device(old_device_name, new_device_name)

            self.__save_persistent_devices()
            self.__load_persistent_devices()
            log.debug("Current renamed_devices dict: %s", self.__renamed_devices)
        else:
            log.debug("Received renamed device notification %r, but device renaming handle is disabled", renamed_device)

    def get_config_path(self):
        return self._config_dir

    def subscribe_to_required_topics(self):
        self.tb_client.client.clean_device_sub_dict()

    def request_device_attributes(self, device_name, shared_keys, client_keys, callback):
        if client_keys is not None:
            self.tb_client.client.gw_request_client_attributes(device_name, client_keys, callback)
        if shared_keys is not None:
            self.tb_client.client.gw_request_shared_attributes(device_name, shared_keys, callback)

    @staticmethod
    def _generate_persistent_key(connector, connectors_persistent_keys):
        if connectors_persistent_keys and connectors_persistent_keys.get(connector['name']) is not None:
            connector_persistent_key = connectors_persistent_keys[connector['name']]
        else:
            connector_persistent_key = "".join(choice(hexdigits) for _ in range(10))
            connectors_persistent_keys[connector['name']] = connector_persistent_key

        return connector_persistent_key

    def _load_connectors(self):
        self.connectors_configs = {}
        connectors_persistent_keys = self.__load_persistent_connector_keys()
        if self.__config.get("connectors"):
            for connector in self.__config['connectors']:
                try:
                    config_file_path = self._config_dir + connector['configuration']

                    with open(config_file_path, 'r', encoding="UTF-8") as conf_file:
                        connector_conf_file_data = conf_file.read()

                    connector_conf = connector_conf_file_data
                    try:
                        connector_conf = loads(connector_conf_file_data)
                    except JSONDecodeError as e:
                        log.debug(e)
                        log.warning("Cannot parse connector configuration as a JSON, it will be passed as a string.")

                    if not self.connectors_configs.get(connector['type']):
                        connector_class = TBModuleLoader.import_module(connector['type'],
                                                                       self._default_connectors.get(
                                                                           connector['type'],
                                                                           'mqtt'))
                        self.connectors_configs[connector['type']] = []
                        self._implemented_connectors[connector['type']] = connector_class
                    if isinstance(connector_conf, dict):
                        connector_conf["name"] = connector['name']
                    self.connectors_configs[connector['type']].append({"name": connector['name'],
                                                                       "config": {connector[
                                                                                      'configuration']: connector_conf},
                                                                       "config_updated": stat(config_file_path),
                                                                       "config_file_path": config_file_path})
                except Exception as e:
                    log.exception("Error on loading connector: %r", e)
            if connectors_persistent_keys:
                self.__save_persistent_keys(connectors_persistent_keys)
        else:
            log.error("Connectors - not found! Check your configuration!")

    def _connect_with_connectors(self):
        for connector_type in self.connectors_configs:
            for connector_config in self.connectors_configs[connector_type]:
                if not self._implemented_connectors.get(connector_type.lower()):
                    continue
                for config in connector_config["config"]:
                    connector = None
                    try:
                        if connector_config["config"][config] is not None:
                            connector = self._implemented_connectors[connector_type](self,
                                                                                     connector_config["config"][
                                                                                         config],
                                                                                     connector_type)
                            connector.setName(connector_config["name"])
                            self.available_connectors[connector.get_name()] = connector
                            connector.open()
                        else:
                            log.info("Config not found for %s", connector_type)
                    except Exception as e:
                        log.exception(e)
                        if connector is not None:
                            connector.close()

    def check_connector_configuration_updates(self):
        configuration_changed = False
        for connector_type in self.connectors_configs:
            for connector_config in self.connectors_configs[connector_type]:
                if stat(connector_config["config_file_path"]) != connector_config["config_updated"]:
                    configuration_changed = True
                    break
            if configuration_changed:
                break
        if configuration_changed:
            self.__close_connectors()
            self._load_connectors()
            self._connect_with_connectors()

    def send_to_storage(self, connector_name, data):
        try:
            device_valid = True
            if self.__device_filter:
                device_valid = self.__device_filter.validate_device(connector_name, data)

            if not device_valid:
                log.warning('Device %s forbidden', data['deviceName'])
                return Status.FORBIDDEN_DEVICE

            filtered_data = self.__duplicate_detector.filter_data(connector_name, data)
            if filtered_data:
                self.__converted_data_queue.put((connector_name, filtered_data), True, 100)
                return Status.SUCCESS
            else:
                return Status.NO_NEW_DATA
        except Exception as e:
            log.exception("Cannot put converted data!", e)
            return Status.FAILURE

    def __send_to_storage(self):
        while not self.stopped:
            try:
                if not self.__converted_data_queue.empty():
                    connector_name, event = self.__converted_data_queue.get(True, 100)
                    data_array = event if isinstance(event, list) else [event]
                    for data in data_array:
                        if not connector_name == self.name:
                            if 'telemetry' not in data:
                                data['telemetry'] = []
                            if 'attributes' not in data:
                                data['attributes'] = []
                            if not TBUtility.validate_converted_data(data):
                                log.error("Data from %s connector is invalid.", connector_name)
                                continue
                            if data.get('deviceType') is None:
                                device_name = data['deviceName']
                                if self.__connected_devices.get(device_name) is not None:
                                    data["deviceType"] = self.__connected_devices[device_name]['device_type']
                                elif self.__saved_devices.get(device_name) is not None:
                                    data["deviceType"] = self.__saved_devices[device_name]['device_type']
                                else:
                                    data["deviceType"] = "default"
                            if data["deviceName"] not in self.get_devices() and self.tb_client.is_connected():
                                self.add_device(data["deviceName"],
                                                {"connector": self.available_connectors[connector_name]},
                                                device_type=data["deviceType"])
                            if not self.__connector_incoming_messages.get(connector_name):
                                self.__connector_incoming_messages[connector_name] = 0
                            else:
                                self.__connector_incoming_messages[connector_name] += 1
                        else:
                            data["deviceName"] = "currentThingsBoardGateway"
                            data['deviceType'] = "gateway"

                        data = self.__convert_telemetry_to_ts(data)

                        max_data_size = self.__config["thingsboard"].get("maxPayloadSizeBytes", 400)
                        if self.__get_data_size(data) >= max_data_size:
                            # Data is too large, so we will attempt to send in pieces
                            adopted_data = {"deviceName": data['deviceName'],
                                            "deviceType": data['deviceType'],
                                            "attributes": {},
                                            "telemetry": []}
                            empty_adopted_data_size = self.__get_data_size(adopted_data)
                            adopted_data_size = empty_adopted_data_size

                            # First, loop through the attributes
                            for attribute in data['attributes']:
                                adopted_data['attributes'].update(attribute)
                                adopted_data_size += self.__get_data_size(attribute)
                                if adopted_data_size >= max_data_size:
                                    # We have surpassed the max_data_size, so send what we have and clear attributes
                                    self.__send_data_pack_to_storage(adopted_data, connector_name)
                                    adopted_data['attributes'] = {}
                                    adopted_data_size = empty_adopted_data_size

                            # Now, loop through telemetry. Possibly have some unsent attributes that have been adopted.
                            if isinstance(data['telemetry'], list):
                                telemetry = data['telemetry']
                            else:
                                telemetry = [data['telemetry']]
                            ts_to_index = {}
                            for ts_kv_list in telemetry:
                                ts = ts_kv_list['ts']
                                for kv in ts_kv_list['values']:
                                    if ts in ts_to_index:
                                        kv_data = {kv: ts_kv_list['values'][kv]}
                                        adopted_data['telemetry'][ts_to_index[ts]]['values'].update(kv_data)
                                    else:
                                        kv_data = {'ts': ts, 'values': {kv: ts_kv_list['values'][kv]}}
                                        adopted_data['telemetry'].append(kv_data)
                                        ts_to_index[ts] = len(adopted_data['telemetry']) - 1

                                    adopted_data_size += self.__get_data_size(kv_data)
                                    if adopted_data_size >= max_data_size:
                                        self.__send_data_pack_to_storage(adopted_data, connector_name)
                                        adopted_data['telemetry'] = []
                                        adopted_data['attributes'] = {}
                                        adopted_data_size = empty_adopted_data_size
                                        ts_to_index = {}

                            if len(adopted_data['telemetry']) > 0 or len(adopted_data['attributes']) > 0:
                                self.__send_data_pack_to_storage(adopted_data, connector_name)
                                adopted_data['telemetry'] = []
                                adopted_data['attributes'] = {}
                        else:
                            self.__send_data_pack_to_storage(data, connector_name)

                else:
                    sleep(0.2)
            except Exception as e:
                log.error(e)

    @staticmethod
    def __get_data_size(data: dict):
        return getsizeof(str(data))

    @staticmethod
    def __convert_telemetry_to_ts(data):
        telemetry = {}
        telemetry_with_ts = []
        for item in data["telemetry"]:
            if item.get("ts") is None:
                telemetry = {**telemetry, **item}
            else:
                telemetry_with_ts.append({"ts": item["ts"], "values": {**item["values"]}})
        if telemetry_with_ts:
            data["telemetry"] = telemetry_with_ts
        elif len(data['telemetry']) > 0:
            data["telemetry"] = {"ts": int(time() * 1000), "values": telemetry}
        return data

    def __send_data_pack_to_storage(self, data, connector_name):
        json_data = dumps(data)
        save_result = self._event_storage.put(json_data)
        if not save_result:
            log.error('Data from the device "%s" cannot be saved, connector name is %s.',
                      data["deviceName"],
                      connector_name)

    def check_size(self, devices_data_in_event_pack):
        if self.__get_data_size(devices_data_in_event_pack) >= self.__config["thingsboard"].get("maxPayloadSizeBytes",
                                                                                                400):
            for device in devices_data_in_event_pack:
                devices_data_in_event_pack[device]["telemetry"] = []
                devices_data_in_event_pack[device]["attributes"] = {}

    def __read_data_from_storage(self):
        devices_data_in_event_pack = {}
        log.debug("Send data Thread has been started successfully.")

        while not self.stopped:
            try:
                if not self.tb_client.is_connected():
                    sleep(self.__min_pack_send_delay_ms)
                    continue

                events = self._event_storage.get_event_pack()

                if events is None or len(events) < 1:
                    sleep(self.__min_pack_send_delay_ms)
                    continue

                for event in events:
                    self.counter += 1
                    try:
                        current_event = loads(event)
                    except Exception as e:
                        log.exception(e)
                        continue

                    if not devices_data_in_event_pack.get(current_event["deviceName"]):
                        devices_data_in_event_pack[current_event["deviceName"]] = {"telemetry": [],
                                                                                   "attributes": {}}
                    if current_event.get("telemetry"):
                        if isinstance(current_event["telemetry"], list):
                            for item in current_event["telemetry"]:
                                self.check_size(devices_data_in_event_pack)
                                devices_data_in_event_pack[current_event["deviceName"]]["telemetry"].append(
                                    item)
                        else:
                            self.check_size(devices_data_in_event_pack)
                            devices_data_in_event_pack[current_event["deviceName"]]["telemetry"].append(
                                current_event["telemetry"])
                    if current_event.get("attributes"):
                        if isinstance(current_event["attributes"], list):
                            for item in current_event["attributes"]:
                                self.check_size(devices_data_in_event_pack)
                                devices_data_in_event_pack[current_event["deviceName"]]["attributes"].update(
                                    item.items())
                        else:
                            self.check_size(devices_data_in_event_pack)
                            devices_data_in_event_pack[current_event["deviceName"]]["attributes"].update(
                                current_event["attributes"].items())
                if devices_data_in_event_pack:
                    sleep(self.__min_pack_send_delay_ms)

                if not self.tb_client.is_connected():
                    continue

                success = True
                while not self._published_events.empty():
                    events = [self._published_events.get(False, 10) for _ in
                              range(min(self.__min_pack_size_to_send, self._published_events.qsize()))]
                    for event in events:
                        try:
                            if self.tb_client.is_connected():
                                if self.tb_client.client.quality_of_service == 1:
                                    success = event.get() == event.TB_ERR_SUCCESS
                                else:
                                    success = True
                            else:
                                break
                        except Exception as e:
                            log.exception(e)
                            success = False
                        sleep(self.__min_pack_send_delay_ms)
                if success and self.tb_client.is_connected():
                    self._event_storage.event_pack_processing_done()
                    del devices_data_in_event_pack
                    devices_data_in_event_pack = {}
            except Exception as e:
                log.exception(e)
                sleep(1)

    def add_device_async(self, data):
        if data['deviceName'] not in self.__saved_devices:
            self.__async_device_actions_queue.put((DeviceActions.CONNECT, data))
            return Status.SUCCESS
        else:
            return Status.FAILURE

    def add_device(self, device_name, content, device_type=None):
        if device_name not in self.__saved_devices:
            device_type = device_type if device_type is not None else 'default'
            self.__connected_devices[device_name] = {**content, "device_type": device_type}
            self.__saved_devices[device_name] = {**content, "device_type": device_type}
            self.__save_persistent_devices()
            self.tb_client.client.gw_connect_device(device_name, device_type)

    def update_device(self, device_name, event, content):
        if event == 'connector' and self.__connected_devices[device_name].get(event) != content:
            self.__save_persistent_devices()
        self.__connected_devices[device_name][event] = content

    def del_device_async(self, data):
        if data['deviceName'] in self.__saved_devices:
            self.__async_device_actions_queue.put((DeviceActions.DISCONNECT, data))
            return Status.SUCCESS
        else:
            return Status.FAILURE

    def del_device(self, device_name):
        self.tb_client.client.gw_disconnect_device(device_name)
        self.__connected_devices.pop(device_name)
        self.__saved_devices.pop(device_name)
        self.__save_persistent_devices()

    def get_devices(self, connector_name: str = None):
        return self.__connected_devices if connector_name is None else {
            device_name: self.__connected_devices[device_name]["device_type"] for device_name in
            self.__connected_devices.keys() if self.__connected_devices[device_name].get("connector") is not None and
            self.__connected_devices[device_name]["connector"].get_name() == connector_name}

    def __process_async_device_actions(self):
        while not self.stopped:
            if not self.__async_device_actions_queue.empty():
                action, data = self.__async_device_actions_queue.get()
                if action == DeviceActions.CONNECT:
                    self.add_device(data['deviceName'], {CONNECTOR_PARAMETER: self.available_connectors[data['name']]},
                                    data.get('deviceType'))
                elif action == DeviceActions.DISCONNECT:
                    self.del_device(data['deviceName'])
            else:
                sleep(.2)

    def __load_persistent_connector_keys(self):
        persistent_keys = {}
        if PERSISTENT_GRPC_CONNECTORS_KEY_FILENAME in listdir(self._config_dir) and \
                path.getsize(self._config_dir + PERSISTENT_GRPC_CONNECTORS_KEY_FILENAME) > 0:
            try:
                persistent_keys = load_file(self._config_dir + PERSISTENT_GRPC_CONNECTORS_KEY_FILENAME)
            except Exception as e:
                log.exception(e)
            log.debug("Loaded keys: %s", persistent_keys)
        else:
            log.debug("Persistent keys file not found")
        return persistent_keys

    def __save_persistent_keys(self, persistent_keys):
        try:
            with open(self._config_dir + PERSISTENT_GRPC_CONNECTORS_KEY_FILENAME, 'w') as persistent_keys_file:
                persistent_keys_file.write(dumps(persistent_keys, indent=2, sort_keys=True))
        except Exception as e:
            log.exception(e)

    def __load_persistent_devices(self):
        devices = None
        if CONNECTED_DEVICES_FILENAME in listdir(self._config_dir) and \
                path.getsize(self._config_dir + CONNECTED_DEVICES_FILENAME) > 0:
            try:
                devices = load_file(self._config_dir + CONNECTED_DEVICES_FILENAME)
            except Exception as e:
                log.exception(e)
        else:
            open(self._config_dir + CONNECTED_DEVICES_FILENAME, 'w').close()

        if devices is not None:
            log.debug("Loaded devices:\n %s", devices)
            for device_name in devices:
                try:
                    if not isinstance(devices[device_name], list):
                        open(self._config_dir + CONNECTED_DEVICES_FILENAME, 'w').close()
                        log.debug("Old connected_devices file, new file will be created")
                        return
                    if self.available_connectors.get(devices[device_name][0]):
                        device_data_to_save = {
                            "connector": self.available_connectors[devices[device_name][0]],
                            "device_type": devices[device_name][1]}
                        if len(devices[device_name]) > 2 and device_name not in self.__renamed_devices:
                            new_device_name = devices[device_name][2]
                            self.__renamed_devices[device_name] = new_device_name
                        self.__connected_devices[device_name] = device_data_to_save
                        self.__saved_devices[device_name] = device_data_to_save
                except Exception as e:
                    log.exception(e)
                    continue
        else:
            log.debug("No device found in connected device file.")
            self.__connected_devices = {} if self.__connected_devices is None else self.__connected_devices

    def __save_persistent_devices(self):
        with self.__lock:
            data_to_save = {}
            for device in self.__connected_devices:
                if self.__connected_devices[device]["connector"] is not None:
                    data_to_save[device] = [self.__connected_devices[device]["connector"].get_name(),
                                            self.__connected_devices[device]["device_type"]]

                    if device in self.__renamed_devices:
                        data_to_save[device].append(self.__renamed_devices.get(device))

            with open(self._config_dir + CONNECTED_DEVICES_FILENAME, 'w') as config_file:
                try:
                    config_file.write(dumps(data_to_save, indent=2, sort_keys=True))
                except Exception as e:
                    log.exception(e)

            log.debug("Saved connected devices.")

    # GETTERS --------------------
    def ping(self):
        return self.name

    # ----------------------------
    # Storage --------------------
    def get_storage_name(self):
        return self._event_storage.__class__.__name__

    def get_storage_events_count(self):
        return self._event_storage.len()

    # Connectors -----------------
    def get_available_connectors(self):
        return {num + 1: name for (num, name) in enumerate(self.available_connectors)}

    def get_connector_status(self, name):
        try:
            connector = self.available_connectors[name]
            return {'connected': connector.is_connected()}
        except KeyError:
            return f'Connector {name} not found!'

    def get_connector_config(self, name):
        try:
            connector = self.available_connectors[name]
            return connector.get_config()
        except KeyError:
            return f'Connector {name} not found!'

    # Gateway ----------------------
    def get_status(self):
        return {'connected': self.tb_client.is_connected()}


if __name__ == '__main__':
    TBGatewayService(
        path.dirname(path.dirname(path.abspath(__file__))) + '/config/tb_gateway.yaml'.replace('/', path.sep))
