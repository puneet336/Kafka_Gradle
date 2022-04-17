package io.conduktor.demos.kafka;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.util.Arrays;
import java.time.Duration;
public class ConsumerDemo {
	private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
	public static void main(String[] args) {
		//System.out.println("Hello World!!?");
		log.info("I am kafka consumer!");
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.29.208:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"JJ2");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
		//create consumer
		KafkaConsumer<String,String> consumer= new KafkaConsumer<>(properties);
		//subscribe to topic
		consumer.subscribe(Arrays.asList("first_topic") );
			
		//poll for new data
		while(true)
		{
			ConsumerRecords<String,String>  records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String,String>  record : records )
			{
				log.info("Key: "+record.key()+" ,Value: "+record.value());
				log.info("Partition: "+record.partition()+" ,Offset: "+ record.offset());
			}
			
		}
		


	}

}
