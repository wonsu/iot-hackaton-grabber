import sys
import json
import time

import paho.mqtt.client
import requests


CITY = 'lodz'
MQTT_CLIENT_ID = f'przeklient{CITY}'
MQTT_HOST = '10.93.1.22'
MQTT_TOPIC = f'/stx/office/{CITY}/air_quality/0'
MQTT_QOS = 2

ENDPOINT_URL = 'http://10.93.8.191:8000/measurements-receiver/'

previous_payload = {}


def on_connect(client, userdata, flags, rc):
    print('connected (%s)' % client._client_id)
    client.subscribe(topic=MQTT_TOPIC, qos=MQTT_QOS)


def on_message(client, userdata, message):
    print('------------------------------')
    print('topic: %s' % message.topic)
    print('payload: %s' % message.payload)
    print('qos: %d' % message.qos)

    data = json.loads(message.payload.decode("utf-8"))
    pm = data["payload"]["particulate_matter"]

    payload = {
        'humidity': data['payload']['humidity'],
        'temperature': data['payload']['temperature'],
        'pm10': pm['pm10'],
        'pm25': pm['pm25'],
        'pm100': pm['pm100'],
    }

    global previous_payload

    if json.dumps(payload) != json.dumps(previous_payload):
        try:
            r = requests.post(ENDPOINT_URL, json=payload)
            print(r.status_code, r.reason)
        except:
            time.sleep(10)


def main():
    client = paho.mqtt.client.Client(client_id=MQTT_CLIENT_ID, clean_session=False)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(host=MQTT_HOST)
    client.loop_forever()


if __name__ == '__main__':
    main()
sys.exit(0)
