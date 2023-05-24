import json

import paho.mqtt.client as mqtt

from mqtt_client import MQTTClient
from alarm_manager import AlarmManager



client = MQTTClient(
    hostname="host",
    url="mosquitto.karina-huinno.tk",
    port=1883,
    token=None
)

__alarm_manager = AlarmManager(client)

def handle_hr_message(client, userdata, msg):
    payload = msg.payload.decode()
    payload_slice = payload.split(":")
    params = payload_slice[1]
    params_slice = params.split(",")
    sensor_type = payload_slice[0][5:]
    value = params_slice[1]

    __alarm_manager.check_alarm( params_slice[0].split("=")[1], sensor_type, value)

def handle_alarm_message(client, userdata, msg):
    payload = json.loads(msg.payload.decode())
    __alarm_manager.upsert_alarm(msg.topic, payload)

def handle_alarm_rule_message(client, userdata, msg):
    payload = json.loads(msg.payload.decode())
    if 'from' in payload and payload['from'] == 'pmc' :
        __alarm_manager.get_alarm_rule()
    else :
        __alarm_manager.upsert_alarm_rule(payload)

def handle_reload_message(client, userdata, msg):
    __alarm_manager.get_exam_with_serial_number()

topic_handlers = {
    "devices/hr": handle_hr_message,
    "alarms/#": handle_alarm_message,
    "noti/alarm-rules/#": handle_alarm_rule_message,
    "noti/reload": handle_reload_message
}

def on_message(client, userdata, msg):
    topic = msg.topic
    for topic_pattern, handler in topic_handlers.items():
        if mqtt.topic_matches_sub(topic_pattern, topic):
            handler(client, userdata, msg)

client.connect()

for topic in topic_handlers:
    client.sub(topic)

client.client.on_message = on_message

while True:
    pass


client.disconnect()

