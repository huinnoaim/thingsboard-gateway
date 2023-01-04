import time
from re import findall
import re
import base64
from re import search
from simplejson import dumps
from thingsboard_gateway.connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter, log
from thingsboard_gateway.gateway.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
import yaml
import os
import numpy as np

from bin_parser import BinReader


SENSOR_ECG_DATA_RECIEVE_SUCCEED = 'memo-web/socket/SENSOR_ECG_DATA_RECIEVE_SUCCEED'
PAYLOAD= {
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
class BytesMqttUplinkConverter(MqttUplinkConverter):
    def __init__(self, config):
        self.__config = config.get('converter')

    @property
    def config(self):
        return self.__config

    @StatisticsService.CollectStatistics(start_stat_type='receivedBytesFromDevices',
                                         end_stat_type='convertedBytesFromDevice')
    def convert(self, topic, data):
        datatypes = {"timeseries": "telemetry"}
        dict_result = {
            'deviceName': '',
            # "deviceName": self.parse_device_name(topic, data, self.__config),
            "telemetry": []
        }

        try:
            for datatype in datatypes:
                dict_result[datatypes[datatype]] = []
                for datatype_config in self.__config.get(datatype, []):
                    value_item = self.parse_data(self, datatype_config['value'], data)
                    if datatype == 'timeseries':
                        dict_result[datatypes[datatype]].append({"ts": int(time.time()) * 1000, 'values': value_item})
                    else:
                        dict_result[datatypes[datatype]].append(value_item)
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), str(data))
            log.exception(e)

        log.debug('Converted data: %s', dict_result)
        dict_result['deviceName'] = dict_result['telemetry'][0]['values']['serialNumber']
        return dict_result

    @staticmethod
    def parse_data(self, expression, data):
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
            result = self.parse_payload(data)
        return result

    @staticmethod
    def parse_payload(data):
        payload_slice = data.split(":")
        operation = payload_slice[0]
        params = payload_slice[1]

        params_slice = params.split(",")
        serial_number = params_slice[0].split("=")[1]
        isEncrypted = params_slice[1]
        ecgData = params_slice[2]
        ecg064data = [1,2,3,4]
        concatEcgData = []

        def headerParser(headerBin):
            parser = BinReader(
                headerBin[:16],
                yaml.safe_load(open(os.getcwd()+'/thingsboard_gateway/connectors/mqtt/structure.yml')),
                yaml.safe_load(open(os.getcwd()+'/thingsboard_gateway/connectors/mqtt/types.yml')))
            return parser.parsed

        def bodyParser(bodyBin):
            body = []
            for x in range(0, len(bodyBin), 3) :
                hh = bodyBin[x]
                mm = bodyBin[x+1]
                ll = bodyBin[x+2]

                v1 = (hh<<4) + ((mm&0xF0)>>4)
                v2 = ((mm&0x0F)<<8) + ll

                if v1>2047:
                    v1 = v1- 0xFFF -1      
            
                if v2>2047:
                    v2 = v2- 0xFFF -1    

                v1 = (v1 * 3.05) / 1000
                if v1 < -4:
                    v1 = -4
                elif v1 > 4:
                    v1 = 4

                v2 = (v2 * 3.05) / 1000
                
                body.append(v1)
                body.append(v2)
            return body

        def ecgParser(str):
            bin = base64.b64decode(re.sub('/[ \r\n]+$/', '', str))
            header = headerParser(bin[:16])
            body = bodyParser(bin[16:])

            header['ecgdata'] = body
            return header

        if isEncrypted == PAYLOAD["PARAM"]["ENCRYPTED"] and operation == PAYLOAD["PARAM"]["MONITORING"]:
            expected_packetLength = PAYLOAD["ECG_DATA"]["BASE64_ECG_DATA_FRAGMENT_LENGTH"] * PAYLOAD["ECG_DATA"]["BASE64_ECG_DATA_FRAGMENT_COUNT"]
            if len(ecgData) == expected_packetLength:
                fragment = ''
                offset = 0
                for i in range(PAYLOAD["ECG_DATA"]["BASE64_ECG_DATA_FRAGMENT_COUNT"]):
                    offset = i * PAYLOAD["ECG_DATA"]["BASE64_ECG_DATA_FRAGMENT_LENGTH"];
                    slice_obj = slice(offset,offset + PAYLOAD["ECG_DATA"]["BASE64_ECG_DATA_FRAGMENT_LENGTH"])
                    fragment = ecgData[slice_obj]
                    ecg064data[i] = ecgParser(fragment);
            else:
                return None
        else:
            return None

        for x in range(PAYLOAD["ECG_DATA"]["BASE64_ECG_DATA_FRAGMENT_COUNT"]):
            concatEcgData = concatEcgData + ecg064data[x]['ecgdata']

        data = {
            'type': SENSOR_ECG_DATA_RECIEVE_SUCCEED,
            'serialNumber': serial_number,
            'timestamp': ecg064data[0]['timestamp'],
            'mataData': ecg064data[0]['metadata'],
            'ecgDataIndex': ecg064data[0]['ecgdataindex'],
            'metaDataType': ecg064data[0]['metadatatype'],
            'ecgData': concatEcgData
        }
        return data
    @staticmethod
    def parse_device_name(topic, data, config):
        return BytesMqttUplinkConverter.parse_device_info(
            topic, data, config, "deviceNameJsonExpression", "deviceNameTopicExpression")

    @staticmethod
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
                if search_result is not None:
                    result = search_result.group(0)
                else:
                    log.debug(
                        "Regular expression result is None. deviceNameTopicExpression parameter will be interpreted "
                        "as a deviceName\n Topic: %s\nRegex: %s", topic, expression)
                    result = expression
            else:
                log.error("The expression for looking \"deviceName\" not found in config %s", dumps(config))
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(config), data)
            log.exception(e)
        return result