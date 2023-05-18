import paho.mqtt.client as mqtt

class MQTTClient:
    def __init__(self, url, port, token):
        self.url = url
        self.port = port
        self.token = token
        self.client = mqtt.Client()

    def on_connect(self, client, userdata, flags, rc):
        print(f"Connected with result code {rc}")

    def on_publish(self, client, userdata, mid):
        print("Message published")

    def connect(self):
        self.client.username_pw_set(self.token, "")
        self.client.on_connect = self.on_connect
        self.client.on_publish = self.on_publish
        self.client.connect(self.url, self.port)
        self.client.loop_start()

    def pub(self, topic, message, qos=1):
        self.client.publish(topic, message, qos)

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()
