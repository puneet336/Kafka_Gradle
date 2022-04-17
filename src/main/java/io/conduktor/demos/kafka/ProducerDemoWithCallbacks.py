#!/usr/bin/env python3
from confluent_kafka import Producer
import socket
import logging


logging.basicConfig(level=logging.DEBUG)
logging.info("Hello World!");


def callback(err,msg):
    if err == None:
        logging.info('Received new metadata')
        logging.info("Topicz: "+ str(msg.topic()))
        logging.info("Partition: "+str(msg.partition()))
        logging.info("Offset: "+str(msg.offset()))
        logging.info("Timestamp: "+str(msg.timestamp()))
    else:
        logging.error("Error:"+err.reason)

conf = {'bootstrap.servers': "192.168.29.208:9092",
        'client.id': socket.gethostname()}
#Properties properties= new Properties();
#properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.29.208:9092");
#properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
#properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

producer = Producer(conf)
#KafkaProducer<String,String> producer = new KafkaProducer <>(properties);
#ProducerRecord<String,String> producerRecord= new ProducerRecord<>("first_topic","hello world");
for i in range(10):

    producer.produce(topic="first_topic", value="hello world py"+str(i),on_delivery=callback)
    #producer.send(producerRecord);


#                //asyncronous
#                producer.send(producerRecord, new Callback(){
#                @Override
#                public void onCompletion(RecordMetadata metadata, Exception e){
#                // message transfer success or exception thrown!
#                if ( e == null)
#                {
#                        log.info("Received new metadata\n"+
#                                "Topicz: "+metadata.topic()+"\n"+
#                                "Partition: "+metadata.partition()+"\n"+
#                                "Offset: "+metadata.offset()+"\n"+
#                                "Timestamp: "+metadata.timestamp()
#                                );
#                }
#                else{
#                        log.error("Error while producing",e);
#                }
#
#                }
#                });


producer.flush()
#producer.flush();

#producer.close()
#producer.close();
