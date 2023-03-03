import base64
import json
import os
import re
import time
from re import findall
from re import search

import yaml
from bin_parser import BinReader
from simplejson import dumps

from thingsboard_gateway.connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter, log
from thingsboard_gateway.gateway.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from cache3 import SafeCache

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

log.info(structure, types)

# key: name, value: cache
# stream_buffer = dict()
ecg_cache = SafeCache()

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
    is_encrypted = params_slice[1]
    ecg_data = params_slice[2]
    ecg064data = [1, 2, 3, 4]
    concat_ecg_data = []

    if is_encrypted != PAYLOAD["PARAM"]["ENCRYPTED"] or operation != PAYLOAD["PARAM"]["MONITORING"]:
        return None

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

    data = {
        'type': SENSOR_ECG_DATA_RECEIVE_SUCCEED,
        'serialNumber': serial_number,
        'timestamp': ecg064data[0]['timestamp'],
        'mataData': ecg064data[0]['metadata'],
        'ecgDataIndex': ecg064data[0]['ecgdataindex'],
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


class BytesMqttUplinkConverter(MqttUplinkConverter):
    def __init__(self, config):
        self.__config = config.get('converter')
        log.info('*****conveter contructing...')

    @property
    def config(self):
        return self.__config

    @StatisticsService.CollectStatistics(start_stat_type='receivedBytesFromDevices',
                                         end_stat_type='convertedBytesFromDevice')
    def convert(self, topic, data):
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
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), str(data))
            log.exception(e)

        # Extract the device name from the telemetry data
        if dict_result['telemetry']:
            dict_result['deviceName'] = dict_result['telemetry'][0]['values']['serialNumber']

        log.debug('Converted data: %s', dict_result)

        device_name = str(dict_result['deviceName'])
        field_ts = str(dict_result['telemetry'][0]['ts'])
        field_ecg = json.dumps(dict_result['telemetry'][0]['values']['ecgData'])

        log.info('device_name: ' + device_name + ', ts: ' + field_ts + ',ecg: ' + field_ecg)

        # valid with 10s
        ecg_cache.ex_set(field_ts, field_ecg, timeout=10, tag=device_name)
        log.info(list(ecg_cache))
        log.info(len(list(ecg_cache)))
        cached = ecg_cache.get_many([i for i in range(10)], tag=device_name)
        log.info(cached)

        # calculate HR


        return dict_result
