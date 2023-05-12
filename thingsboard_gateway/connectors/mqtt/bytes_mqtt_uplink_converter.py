import base64
import json
import os
import re
import time
import numpy as np
from timeit import default_timer as timer
import asyncio
from re import findall
from re import search

import yaml
from bin_parser import BinReader
from simplejson import dumps
from cache3 import SafeCache, SimpleCache

from thingsboard_gateway.connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
import thingsboard_gateway.connectors.mqtt.hr_detector as hr_detector
from thingsboard_gateway.connectors.mqtt.alarm_manager import AlarmManager
from thingsboard_gateway.connectors.mqtt.http_manager import HttpManager
import logging

log = logging.getLogger("converter")

HR_CALC_RANGE_SEC = 10  # 10sec
AI_INPUT_ECG_LENGTH = 15000  # 1min

SENSOR_ECG_DATA_RECEIVE_SUCCEED = 'memo-web/socket/SENSOR_ECG_DATA_RECEIVE_SUCCEED'
PAYLOAD = {
    'PARAM': {
        'MONITORING': 'noti=telemetry',
        'ENCRYPTED': '0',
        'NO_ENCRYPTED': '0'
    },
    'ECG_DATA': {
        'BASE64_ECG_DATA_FRAGMENT_LENGTH': 344,
        'BASE64_ECG_DATA_FRAGMENT_COUNT': 4,
    },
}

path_current_directory = os.path.dirname(__file__)
path_structure_yml = os.path.join(path_current_directory, 'structure.yml')
path_types_yml = os.path.join(path_current_directory, 'types.yml')
structure = yaml.safe_load(open(path_structure_yml).read())
types = yaml.safe_load(open(path_types_yml).read())

log.debug(structure, types)

BUFFER_TIMEOUT = 80
# key: name, value: cache
ecg_cache = SafeCache()
ttl_cache = SimpleCache('ttl', BUFFER_TIMEOUT)
index_cache = SimpleCache('index', BUFFER_TIMEOUT)


def parse_data(expression, data):
    parsed = None
    expression_arr = findall(r'\[\S[0-9:]*]', expression)
    converted_data = expression

    for exp in expression_arr:
        indexes = exp[1:-1].split(':')

        data_to_replace = ''
        if len(indexes) == 2:
            from_index, to_index = indexes
            concat_arr = data[
                         int(from_index) if from_index != '' else None:int(
                             to_index) if to_index != '' else None]
            for sub_item in concat_arr:
                data_to_replace += str(sub_item)
        else:
            data_to_replace += str(data[int(indexes[0])])

        converted_data = converted_data.replace(exp, data_to_replace)
        parsed = parse_payload(data)
    return parsed


def parse_header(header_encoded):
    parser = None
    try:
        parser = BinReader(
            header_encoded,
            structure,
            types
        )
    except Exception as e:
        log.error(e)
    return parser.parsed


def parse_body(body_encoded):
    body = []
    # 3 bytes represents 2 ECG value
    for x in range(0, len(body_encoded), 3):
        hh = body_encoded[x]
        mm = body_encoded[x + 1]
        ll = body_encoded[x + 2]

        v1 = (hh << 4) + ((mm & 0xF0) >> 4)
        v2 = ((mm & 0x0F) << 8) + ll

        if v1 > 2047:
            v1 = v1 - 0xFFF - 1

        if v2 > 2047:
            v2 = v2 - 0xFFF - 1

        v1 = (v1 * 3.05) / 1000
        if v1 < -4:
            v1 = -4
        elif v1 > 4:
            v1 = 4

        v2 = (v2 * 3.05) / 1000

        body.append(v1)
        body.append(v2)
    return body


def parse_ecg(ecg_encoded):
    ecg = None
    try:
        decoded = base64.b64decode(re.sub('/[ \r\n]+$/', '', ecg_encoded))
        # fill header info
        ecg = parse_header(decoded[:16])
        body = parse_body(decoded[16:])

        ecg['ecgdata'] = body
    except Exception as e:
        log.error('ECG parsing error', e)
    return ecg


def parse_payload(data):
    payload_slice = data.split(":")
    operation = payload_slice[0]
    params = payload_slice[1]
    params_slice = params.split(",")
    serial_number = params_slice[0].split("=")[1]

    # noti : spo2, bt, nbp

    if operation[5:] == 'bt':
        value = params_slice[1]
        data = {
            'type': SENSOR_ECG_DATA_RECEIVE_SUCCEED,
            'serialNumber': serial_number,
            'bt': value
        }
        return data
    elif operation[5:] == 'spo2':
        value = params_slice[1]
        data = {
            'type': SENSOR_ECG_DATA_RECEIVE_SUCCEED,
            'serialNumber': serial_number,
            'spo2': value
        }
        return data
    elif operation[5:] == 'nbp':
        data = {
            'type': SENSOR_ECG_DATA_RECEIVE_SUCCEED,
            'serialNumber': serial_number,
            'systolic': params_slice[1],
            'diastolic': params_slice[2],
            'meanArterialPressure': params_slice[3],
            'timestamp': params_slice[4],
        }
        return data
    else:
        is_encrypted = params_slice[1]
        if is_encrypted != PAYLOAD["PARAM"]["ENCRYPTED"] or operation != PAYLOAD["PARAM"]["MONITORING"]:
            return None
        ecg_data = params_slice[2]
        ecg064data = [1, 2, 3, 4]
        concat_ecg_data = []
        expected_packet_length = PAYLOAD["ECG_DATA"]["BASE64_ECG_DATA_FRAGMENT_LENGTH"] * PAYLOAD["ECG_DATA"][
            "BASE64_ECG_DATA_FRAGMENT_COUNT"]

        if len(ecg_data) != expected_packet_length:
            return None

        fragment_count = PAYLOAD["ECG_DATA"]["BASE64_ECG_DATA_FRAGMENT_COUNT"]
        for i in range(fragment_count):
            offset = i * PAYLOAD["ECG_DATA"]["BASE64_ECG_DATA_FRAGMENT_LENGTH"]
            slice_obj = slice(offset, offset + PAYLOAD["ECG_DATA"]["BASE64_ECG_DATA_FRAGMENT_LENGTH"])
            fragment = ecg_data[slice_obj]
            ecg064data[i] = parse_ecg(fragment)
            concat_ecg_data += ecg064data[i]['ecgdata']

        ecg_index = ecg064data[0]['ecgdataindex']
        data = {
            'type': SENSOR_ECG_DATA_RECEIVE_SUCCEED,
            'serialNumber': serial_number,
            'timestamp': ecg064data[0]['timestamp'],
            'mataData': ecg064data[0]['metadata'],
            'ecgDataIndex': ecg_index,
            'metaDataType': ecg064data[0]['metadatatype'],
            'ecgData': concat_ecg_data
        }
        return data


def parse_device_name(topic, data, config):
    return parse_device_info(
        topic, data, config, "deviceNameJsonExpression", "deviceNameTopicExpression")


def parse_device_info(topic, data, config, json_expression_config_name, topic_expression_config_name):
    result = None
    try:
        if config.get(json_expression_config_name) is not None:
            expression = config.get(json_expression_config_name)
            result_tags = TBUtility.get_values(expression, data, get_tag=True)
            result_values = TBUtility.get_values(expression, data, expression_instead_none=True)

            result = expression
            for (result_tag, result_value) in zip(result_tags, result_values):
                is_valid_key = "${" in expression and "}" in expression
                result = result.replace('${' + str(result_tag) + '}',
                                        str(result_value)) if is_valid_key else result_tag
        elif config.get(topic_expression_config_name) is not None:
            expression = config.get(topic_expression_config_name)
            search_result = search(expression, topic)
            if search_result is None:
                log.warning(
                    "Regular expression result is None. deviceNameTopicExpression parameter will be interpreted "
                    "as a deviceName\n Topic: %s\nRegex: %s", topic, expression)
                return expression
            result = search_result.group(0)
        else:
            log.error("The expression for looking \"deviceName\" not found in config %s", dumps(config))
    except Exception as e:
        log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(config), data)
        log.exception(e)
    return result


def fetch_ecg(device_name):
    # 0: ts, 1: index, 2: device name
    ai_input = filter(lambda d: d[2] == device_name, list(ecg_cache))

    ai_input = sorted(ai_input, key=lambda el: el[0], reverse=False)
    # print(list(map(lambda d: d[0], ai_input)))
    ai_input = list(map(lambda d: json.loads(d[1]), ai_input))
    ai_input = np.array(ai_input, float).flatten().tolist()
    # 1min =  250*60 = 15000
    ai_input = ai_input[:AI_INPUT_ECG_LENGTH]
    # print('ECG cache len ' + device_name + ' : ' + str(len(list(ecg_cache))))
    # log.debug('ECG cache value length ' + device_name + ' : ' + str(len(list(ai_input))))
    # print(len(ai_input))
    return ai_input


def calculate_hr(device_name):
    # calculate HR
    # now_ms = int( time.time_ns() / 1000 )
    now_sec = int(time.time())
    hr_input = filter(lambda d: (d[2] == device_name and (int(d[0]) >= now_sec - HR_CALC_RANGE_SEC)), list(ecg_cache))
    # print(len(list(hr_input)))
    # print(list(hr_input))
    # hr_input tuple = (timestamp, ecg, device_name)
    hr_input = sorted(hr_input, key=lambda el: el[0], reverse=False)
    # print(list(map(lambda d: d[0], hr_input)))
    hr_input = list(map(lambda d: json.loads(d[1]), hr_input))
    hr_input = np.array(hr_input, float).flatten().tolist()
    # 3200개 / 5 번, 11초  = 640개/1번 = 320개/1초
    # 2560개 / 4 번, 10초 = 640개/1번 = 약256개/1초
    # print(len(hr_input))
    # print(list(hr_input))
    # 250 samples/s
    hr = hr_detector.detect(hr_input, 250)
    log.debug(device_name + ', HR: ' + str(hr))
    return hr


class BytesMqttUplinkConverter(MqttUplinkConverter):
    def __init__(self, config, ai_queue, trigger_queue):
        self.__config = config.get('converter')
        self.__loop = asyncio.new_event_loop()
        self.__alarm_manager = AlarmManager()
        self.__http_manager = HttpManager(ai_queue, trigger_queue)
        global ecg_cache
        global ttl_cache
        global index_cache

    @property
    def config(self):
        return self.__config

    def convert(self, topic, data):
        start_time = timer()

        # Define the datatypes to be parsed
        datatypes = {'timeseries': 'telemetry'}

        # Initialize the result dictionary with deviceName and telemetry
        dict_result = {'deviceName': '', 'telemetry': []}

        try:
            # Process each datatype in the datatypes dictionary
            for datatype, key in datatypes.items():
                dict_result[key] = []
                for config in self.__config.get(datatype, []):
                    # Parse the value of the datatype using the given configuration
                    value = parse_data(config['value'], data)

                    if datatype == 'timeseries':
                        # For timeseries, add a timestamp in milliseconds
                        ts = int(time.time()) * 1000
                        dict_result[key].append({'ts': ts, 'values': value})
                    else:
                        dict_result[key].append(value)
        except Exception as e:
            # Log the error and the config and data that caused the error
            log.error(e, dumps(self.__config), str(data))
            return dict_result

        # Extract the device name from the telemetry data
        dict_result['deviceName'] = dict_result['telemetry'][0]['values']['serialNumber']

        # log.debug('Converted data: %s', dict_result)

        device_name = str(dict_result['deviceName'])
        field_ts = int(dict_result['telemetry'][0]['ts'])

        if 'ecgData' in dict_result['telemetry'][0]['values']:
            field_ecg = json.dumps(dict_result['telemetry'][0]['values']['ecgData'])
            field_ecg_index = int(dict_result['telemetry'][0]['values']['ecgDataIndex'])

            self.queuing_ecg(device_name, field_ts, field_ecg, field_ecg_index)

            hr = calculate_hr(device_name)

            dict_result['telemetry'][0]['values']['hr'] = hr

        # check alarm
        alarm = self.__alarm_manager.find_alarms_if_met_condition(dict_result)
        if alarm is not None:
            log.debug(alarm)
            dict_result['alarm'] = alarm

        # end_time = timer()
        # log.debug('<<mqtt byte elapsed time>>: ' + str(end_time - start_time))  # Time in seconds, e.g. 5.38091952400282
        return dict_result

    def queuing_ecg(self, device_name, start_ts: int, ecg_list, ecg_index: int):
        field_ts = str(start_ts)
        field_ecg = ecg_list
        field_ecg_index = str(ecg_index)

        # valid with 1m =  1x60
        ecg_cache.ex_set(field_ts, field_ecg, timeout=BUFFER_TIMEOUT, tag=device_name)
        index_cache.ex_set(field_ts, field_ecg_index, timeout=BUFFER_TIMEOUT, tag=device_name)
        # print(index_cache)
        # 0: ts, 1: index, 2: device name
        index_list = filter(lambda d: d[2] == device_name, list(index_cache))
        index_list = sorted(index_list, key=lambda el: el[0], reverse=True)
        # print(index_list)
        if len(index_list) > 1:
            last_ecg_index = int(index_list[1][1])
            expected_index = last_ecg_index + 960
            if expected_index != ecg_index:
                log.warning('MISSING ECG: expected: ' + str(expected_index) + ', received: ' + str(ecg_index))

        # 60s ttl
        if not ttl_cache.has_key(device_name, tag='ttl'):
            ttl_cache.set(device_name, 'ttl', timeout=BUFFER_TIMEOUT, tag='ttl')
            log.debug('Now trigger AI--------')

            ai_input = fetch_ecg(device_name)
            log.debug('ecg size:' + str(len(ai_input)))
            if len(ai_input) == AI_INPUT_ECG_LENGTH:
                log.debug('Upload 15000 AI INPUT')
                # self.__loop.run_until_complete(
                self.__http_manager.upload_ecg(device_name, ai_input)
                # )

        ttl = ttl_cache.ttl(device_name, tag='ttl')
        # 0030T0000200 - ts:1678230848000, ecg_index:12248640, ecg_list:5760
        # log.debug(device_name + ' - ts:' + field_ts + ', ecg_index:' + str(ecg_index) + ', ecg_list_len:' + str(
        #     len(ecg_list)) + ',TTL:' + str(ttl))
        # print(list(ecg_cache))
        # print('ECG cache len ' + device_name + ' : ' + str(len(list(ecg_cache))))
        # print(len(list(ecg_cache)))
