import json
from mqtt_client import MQTTClient
from redis_client import Redis, RedisUtils

redis = Redis.from_cfgfile()
devices = RedisUtils.ECG.get_devices(redis)
for device in devices:
    print(device)
    idxes = RedisUtils.ECG.get_lastest_index(redis, device, 2)
    for key in idxes:
        print(key)
        data = redis.get(key)
        data = json.loads(data)
        # print(data)
    



# mqtt_client = MQTTClient(
#     url="a3480422f7d584c65aaf2b9fa258fead-500139692.ap-northeast-2.elb.amazonaws.com",
#     port=8081,
#     token="pM8bzmflJvCCuvcSKwhB"
# )

# # MQTT 브로커에 연결
# mqtt_client.connect()

# payload = {}
# payload['0023A0000011'] = [{}]
# mqtt_client.pub(
#     topic="v1/gateway/telemetry",
#     message= '{"0023A0000011": [{"ts": 1483228800000,"values": {"temperature": 42,"humidity": 80}},{"ts": 1483228801000,"values": {"temperature": 43,"humidity": 82}}],"0023P1000200": [{"ts": 1483228800000,"values": {"temperature": 42,"humidity": 80}}]}'
# )

# # 연결 종료
# mqtt_client.disconnect()
