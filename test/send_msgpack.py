#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import

import msgpack
import paho.mqtt.client as mqtt

MQTTHOST = "localhost"

client = mqtt.Client(protocol=mqtt.MQTTv311)

client.connect(MQTTHOST)

data = {'a': 10, 'b': 10.0}
packed = msgpack.packb(data)

TOPIC = "mqforward/a/b"
client.publish(TOPIC, bytearray(packed))

import time
time.sleep(0.05)
