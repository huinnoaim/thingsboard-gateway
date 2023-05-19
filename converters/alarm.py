from mqtt_client import MQTTClient
from alarm_manager import AlarmManager
import paho.mqtt.client as mqtt
import json

__alarm_manager = AlarmManager()

def handle_telemetry_message(client, userdata, msg):
    print('dd')


def handle_alarm_message(client, userdata, msg):
    payload = json.loads(msg.payload.decode())
    if payload['originator'] == 'pm' :
        __alarm_manager.upsert_alarm(msg.topic, payload)

topic_handlers = {
    "devices/+/telemetry1": handle_telemetry_message,
    "alarms/#": handle_alarm_message,
}

def on_message(client, userdata, msg):
    topic = msg.topic
    for topic_pattern, handler in topic_handlers.items():
        if mqtt.topic_matches_sub(topic_pattern, topic):
            handler(client, userdata, msg)

client = MQTTClient(
    url="mosquitto.karina-huinno.tk",
    port=1883,
    token=None
)


# 알람 룰 전부 불러오기(n8n에서) + examid 에 맞게 device 시리얼도 같이 긁어와야 매칭가능.
# 텔레메트리가 들어오면 해당하는 룰에 비교해서 맞나 확인하기
# 룰에 비교해서 알람조건에 해당하면 mqtt로 알람 발생시키기 + db에 insert or update하기.


# pmc, pm 에서 알람룰 업데이트 mqtt 도착시
# db에 업데이트 or insert 해주고,
# 룰 관리하는 메모리에서도 다시 로드함.

client.connect()

for topic in topic_handlers:
    client.sub(topic)

client.client.on_message = on_message

while True:
    pass

client.disconnect()