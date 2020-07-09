import paho.mqtt.client as mqtt
from influxdb import client as influxdb
from datetime import datetime
import pytz
from threading import Timer
from collections import defaultdict
import sys
import os


def on_publish(client, userdata, result):
        print("TIMEOUT PUBLISHED!")

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("senselet/#")

# The callback for when a PUBLISH message is received from the server.

def process_ht(topic, message):
    tt =   float(message.split('_')[0])
    temp = float(message.split('_')[1])
    humi = float(message.split('_')[2])
    points = []
    point  = {}
    point['measurement'] = 'temp_humi_measurement'
    point['time']   = datetime.fromtimestamp(tt, pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    point['fields'] = {'temperature': temp, 'humidity': humi}
    point['tags']   = {'sensor': int(topic.split('/')[1].split('_')[0])}
    points.append(point)
    if len(points) > 0:
        influx.write_points(points)


def process_ds(topic, message):
    tt =   float(message.split('_')[0])
    isOpen = float(message.split('_')[1])
    points = []
    point  = {}
    point['measurement'] = 'door_status_measurement'
    point['time']   = datetime.fromtimestamp(tt, pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    point['fields'] = {'doorStatus': isOpen}
    # multi tag?
    point['tags']   = {'sensor': int(topic.split('/')[1].split('_')[0])}
    points.append(point)
    if len(points) > 0:
        influx.write_points(points)


def process_ps(topic, message):
    tt =   float(message.split('_')[0])
    isMovement = float(message.split('_')[1])
    points = []
    point  = {}
    point['measurement'] = 'movement_measurement'
    point['time']   = datetime.fromtimestamp(tt, pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    point['fields'] = {'movementStatus': isMovement}
    # multi tag?
    point['tags']   = {'sensor': int(topic.split('/')[1].split('_')[0])}
    points.append(point)
    if len(points) > 0:
        influx.write_points(points)



def process_wl(topic, message):
    tt =   float(message.split('_')[0])
    isLeak= float(message.split('_')[1])
    points = []
    point  = {}
    point['measurement'] = 'water_leakage_measurement'
    point['time']   = datetime.fromtimestamp(tt, pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    point['fields'] = {'leakstatus': isLeak}
    # multi tag?
    point['tags']   = {'sensor': int(topic.split('/')[1].split('_')[0])}
    points.append(point)
    if len(points) > 0:
        influx.write_points(points)

def process_di(topic, message):
    tt =   float(message.split('_')[0])
    distance = float(message.split('_')[1])
    points = []
    point  = {}
    point['measurement'] = 'distance_measurement'
    point['time']   = datetime.fromtimestamp(tt, pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    point['fields'] = {'distance': distance}
    point['tags']   = {'sensor': int(topic.split('/')[1].split('_')[0])}
    points.append(point)
    if len(points) > 0:
        influx.write_points(points)

switcher = {
    0: process_ht,        
    1: process_ds,
    2: process_ps,
    3: process_wl,
    4: process_di
}

def on_message(client, userdata, msg):
    topic = msg.topic
    message = (msg.payload).decode('ascii')
    print(topic+" "+message)

    sensor_type = int(topic.split('_')[1])
    # process and write data to indluxdb
    # TODO: get switcher working
    # TODO: understand measurement tags
    # process_ds(topic, message)
    switcher.get(sensor_type, "Invalid Sensor Type")(topic, message)



def main():

    global channel, influx, watchdogs #, client_w
    influx = influxdb.InfluxDBClient('localhost', 8086, 'root', 'root', 'senselet')
    dbs = influx.get_list_database()
    db_exists = False
    for db in dbs:
        if db['name'] == 'senselet':
            db_exists = True
            break
    if not db_exists:
        influx.create_database('senselet')

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect("localhost", 1883, 60)
    
    # client_w = mqtt.Client()
    # client_w.on_publish = on_publish

    # client_w.connect("localhost", 1884, 60)

    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.
    client.loop_forever()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
