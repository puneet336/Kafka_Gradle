package io.conduktor.demos.kafka;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;

public class ProducerDemoWithCallbacks {
	private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallbacks.class.getSimpleName());
	public static void main(String[] args) {
		//System.out.println("Hello World!!?");
		log.info("I am a kafka producer");
		//producer properties
		Properties properties= new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.29.208:9092");

		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		
		KafkaProducer<String,String> producer = new KafkaProducer <>(properties);
		for (int i=0;i<10;i++)
		{	
			ProducerRecord<String,String> producerRecord= new ProducerRecord<>("first_topic","hello world "+String.valueOf(i));
			//asyncronous
			producer.send(producerRecord, new Callback(){
			@Override
			public void onCompletion(RecordMetadata metadata, Exception e){
			// message transfer success or exception thrown!
			if ( e == null)
			{
				log.info("Received new metadata\n"+
					"Topicz: "+metadata.topic()+"\n"+
					"Partition: "+metadata.partition()+"\n"+
					"Offset: "+metadata.offset()+"\n"+
					"Timestamp: "+metadata.timestamp()
					);
			}
			else{
				log.error("Error while producing",e);
			}
			}
			});
		}		
		//flush - syncronous
		producer.flush();

		//syncronous
		producer.close();


	}

}
