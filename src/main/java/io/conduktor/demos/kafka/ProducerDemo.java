package io.conduktor.demos.kafka;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
public class ProducerDemo {
	private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
	public static void main(String[] args) {
		//System.out.println("Hello World!!?");
		log.info("Hello World!");
		//producer properties
		Properties properties= new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.29.208:9092");

		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		
		KafkaProducer<String,String> producer = new KafkaProducer <>(properties);
		ProducerRecord<String,String> producerRecord= new ProducerRecord<>("first_topic","hello world");
		
		//asyncronous
		producer.send(producerRecord);
			
		//flush - syncronous
		producer.flush();

		//syncronous
		producer.close();


	}

}
