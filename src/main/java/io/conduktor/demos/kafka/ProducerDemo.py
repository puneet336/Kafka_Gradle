#!/usr/bin/env python3
from confluent_kafka import Producer
import socket
import logging


logging.basicConfig(level=logging.DEBUG)
logging.info("Hello World!");



conf = {'bootstrap.servers': "192.168.29.208:9092",
        'client.id': socket.gethostname()}
#Properties properties= new Properties();
#properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.29.208:9092");
#properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
#properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

producer = Producer(conf)
#KafkaProducer<String,String> producer = new KafkaProducer <>(properties);
#ProducerRecord<String,String> producerRecord= new ProducerRecord<>("first_topic","hello world");

producer.produce(topic="first_topic", key="test", value="hello world")
#producer.send(producerRecord);

producer.flush()
#producer.flush();

#producer.close()
#producer.close();
