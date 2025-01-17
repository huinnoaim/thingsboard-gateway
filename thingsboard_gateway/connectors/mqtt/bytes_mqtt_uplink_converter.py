import base64
import json
import logging
import os
import re
import time
from re import findall
from re import search

import yaml
from bin_parser import BinReader
from simplejson import dumps

import thingsboard_gateway.connectors.mqtt.hr_detector as hr_detector
from thingsboard_gateway.connectors.mqtt.alarm_manager import AlarmManager
from thingsboard_gateway.connectors.mqtt.http_manager import HttpManager
from thingsboard_gateway.connectors.mqtt.mqtt_uplink_converter import (
    MqttUplinkConverter,
)
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

log = logging.getLogger("converter")

SENSOR_ECG_DATA_RECEIVE_SUCCEED = "memo-web/socket/SENSOR_ECG_DATA_RECEIVE_SUCCEED"
PAYLOAD = {
    "PARAM": {"MONITORING": "noti=telemetry", "ENCRYPTED": "0", "NO_ENCRYPTED": "0"},
    "ECG_DATA": {
        "BASE64_ECG_DATA_FRAGMENT_LENGTH": 344,
        "BASE64_ECG_DATA_FRAGMENT_COUNT": 4,
    },
}

path_current_directory = os.path.dirname(__file__)
path_structure_yml = os.path.join(path_current_directory, "structure.yml")
path_types_yml = os.path.join(path_current_directory, "types.yml")
structure = yaml.safe_load(open(path_structure_yml).read())
types = yaml.safe_load(open(path_types_yml).read())

log.debug(structure, types)

BUFFER_TIMEOUT = 80
# Cache.ex_set(self, key: Any, value: Any, timeout: Time = None, tag: TG = None) -> bool:
# key: name, value: cache


def parse_data(expression: str, data: str):
    parsed = None
    expression_arr = findall(r"\[\S[0-9:]*]", expression)
    converted_data = expression

    for exp in expression_arr:
        indexes = exp[1:-1].split(":")

        data_to_replace = ""
        if len(indexes) == 2:
            from_index, to_index = indexes
            concat_arr = data[
                int(from_index)
                if from_index != ""
                else None : int(to_index)
                if to_index != ""
                else None
            ]
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
        parser = BinReader(header_encoded, structure, types)
    except Exception as e:
        log.error(e)
    return parser.parsed


def parse_body(body_encoded: bytes, precision: int = 20):
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

        body.append(round(v1, precision))
        body.append(round(v2, precision))
    return body


def parse_ecg(ecg_encoded):
    ecg = None
    try:
        decoded = base64.b64decode(re.sub("/[ \r\n]+$/", "", ecg_encoded))
        # fill header info
        ecg = parse_header(decoded[:16])
        body = parse_body(decoded[16:], 4)

        ecg["ecgdata"] = body
    except Exception as e:
        log.error("ECG parsing error", e)
    return ecg


def parse_payload(data):
    payload_slice = data.split(":")
    operation = payload_slice[0]
    params = payload_slice[1]
    params_slice = params.split(",")
    serial_number = params_slice[0].split("=")[1]

    # noti : spo2, bt, nbp

    if operation[5:] == "bt":
        value = params_slice[1]
        data = {
            "type": SENSOR_ECG_DATA_RECEIVE_SUCCEED,
            "serialNumber": serial_number,
            "bt": value,
        }
        return data
    elif operation[5:] == "spo2":
        value = params_slice[1]
        data = {
            "type": SENSOR_ECG_DATA_RECEIVE_SUCCEED,
            "serialNumber": serial_number,
            "spo2": value,
        }
        return data
    elif operation[5:] == "nbp":
        data = {
            "type": SENSOR_ECG_DATA_RECEIVE_SUCCEED,
            "serialNumber": serial_number,
            "systolic": params_slice[1],
            "diastolic": params_slice[2],
            "meanArterialPressure": params_slice[3],
            "timestamp": params_slice[4],
        }
        return data
    else:
        is_encrypted = params_slice[1]
        if (
            is_encrypted != PAYLOAD["PARAM"]["ENCRYPTED"]
            or operation != PAYLOAD["PARAM"]["MONITORING"]
        ):
            return None
        ecg_data = params_slice[2]
        ecg064data = [1, 2, 3, 4]
        concat_ecg_data = []
        expected_packet_length = (
            PAYLOAD["ECG_DATA"]["BASE64_ECG_DATA_FRAGMENT_LENGTH"]
            * PAYLOAD["ECG_DATA"]["BASE64_ECG_DATA_FRAGMENT_COUNT"]
        )

        if len(ecg_data) != expected_packet_length:
            return None

        fragment_count = PAYLOAD["ECG_DATA"]["BASE64_ECG_DATA_FRAGMENT_COUNT"]
        for i in range(fragment_count):
            offset = i * PAYLOAD["ECG_DATA"]["BASE64_ECG_DATA_FRAGMENT_LENGTH"]
            slice_obj = slice(
                offset, offset + PAYLOAD["ECG_DATA"]["BASE64_ECG_DATA_FRAGMENT_LENGTH"]
            )
            fragment = ecg_data[slice_obj]
            ecg064data[i] = parse_ecg(fragment)
            concat_ecg_data += ecg064data[i]["ecgdata"]

        ecg_index = ecg064data[0]["ecgdataindex"]
        data = {
            "type": SENSOR_ECG_DATA_RECEIVE_SUCCEED,
            "serialNumber": serial_number,
            "timestamp": ecg064data[0]["timestamp"],
            "mataData": ecg064data[0]["metadata"],
            "ecgDataIndex": ecg_index,
            "metaDataType": ecg064data[0]["metadatatype"],
            "ecgData": concat_ecg_data,
        }
        return data


def parse_device_name(topic, data, config):
    return parse_device_info(
        topic, data, config, "deviceNameJsonExpression", "deviceNameTopicExpression"
    )


def parse_device_info(
    topic, data, config, json_expression_config_name, topic_expression_config_name
):
    result = None
    try:
        if config.get(json_expression_config_name) is not None:
            expression = config.get(json_expression_config_name)
            result_tags = TBUtility.get_values(expression, data, get_tag=True)
            result_values = TBUtility.get_values(
                expression, data, expression_instead_none=True
            )

            result = expression
            for result_tag, result_value in zip(result_tags, result_values):
                is_valid_key = "${" in expression and "}" in expression
                result = (
                    result.replace("${" + str(result_tag) + "}", str(result_value))
                    if is_valid_key
                    else result_tag
                )
        elif config.get(topic_expression_config_name) is not None:
            expression = config.get(topic_expression_config_name)
            search_result = search(expression, topic)
            if search_result is None:
                log.warning(
                    "Regular expression result is None. deviceNameTopicExpression parameter will be interpreted "
                    "as a deviceName\n Topic: %s\nRegex: %s",
                    topic,
                    expression,
                )
                return expression
            result = search_result.group(0)
        else:
            log.error(
                'The expression for looking "deviceName" not found in config %s',
                dumps(config),
            )
    except Exception as e:
        log.error(
            "Error in converter, for config: \n%s\n and message: \n%s\n",
            dumps(config),
            data,
        )
        log.exception(e)
    return result


class BytesMqttUplinkConverter(MqttUplinkConverter):
    def __init__(self, config):
        self.__config = config.get("converter")

    @property
    def config(self):
        return self.__config

    def convert(self, topic, data):
        datatypes = {"timeseries": "telemetry"}
        dict_result = {"deviceName": "", "telemetry": []}

        try:
            for datatype, key in datatypes.items():
                dict_result[key] = []
                for config in self.__config.get(datatype, []):
                    # Parse the value of the datatype using the given configuration
                    value = parse_data(config["value"], data)

                    if datatype == "timeseries":
                        # For timeseries, add a timestamp in milliseconds
                        ts = int(time.time()) * 1000
                        dict_result[key].append({"ts": ts, "values": value})
                    else:
                        dict_result[key].append(value)
        except Exception as e:
            # Log the error and the config and data that caused the error
            log.error(e, dumps(self.__config), str(data))
            return dict_result

        try:
            # Extract the device name from the telemetry data
            dict_result["deviceName"] = dict_result["telemetry"][0]["values"][
                "serialNumber"
            ]
        except TypeError as e:
            log.error(e, dumps(self.__config), str(data))
            return dict_result

        log.info(f"Device: {dict_result['deviceName']}, Data is converted")
        return dict_result
