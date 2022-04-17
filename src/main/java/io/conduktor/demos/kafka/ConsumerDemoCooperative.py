#!/usr/bin/env python3
from confluent_kafka import Consumer
import socket
import logging
import signal
import time
import sys

_running=True
logging.basicConfig(level=logging.DEBUG)
def signal_handler(sig, frame):
    logging.warn("Detected a shutdown!")
    _running=False
    time.sleep(3)
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
conf = {'bootstrap.servers': "192.168.29.208:9092",
        'group.id': "JJ2",
        'auto.offset.reset': 'earliest',
        'partition.assignment.strategy': 'cooperative-sticky' }

consumer = Consumer(conf)
try:
    consumer.subscribe(["first_topic"])

    while _running:
        msg = consumer.poll(timeout=1.0)
        if msg is None: 
            continue
        _key=msg.key()
        if _key == None:
            _key="Blank"
        else:
            _key=msg.key().decode('utf-8')
        _val=msg.value().decode('utf-8')
        logging.info(_key+":"+_val)
finally:
    # Close down consumer to commit final offsets.
    logging.info("closed!")
    consumer.close()


