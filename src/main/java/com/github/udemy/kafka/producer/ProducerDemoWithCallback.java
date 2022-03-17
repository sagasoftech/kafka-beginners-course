package com.github.udemy.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {

	public static void main(String[] args) {

		final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
		
		//String bootstrapServer = "127.0.0.1:9092";
		String bootstrapServer = "localhost:9092";
		
		//Create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		
		//Create producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		for(int i = 0; i < 10; i++) {
			ProducerRecord< String, String> record = new ProducerRecord<String, String>("first-topic", "Hello World "+ i);
			
			//Send Data
			producer.send(record, new Callback() {
				
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					// Execute every time a record is successfully send or produce a error
					
					if(exception==null) {
						//Record was successfully send
						logger.info("Received new metadata. \n" +
									"Topic: " + metadata.topic() + "\n" +
									"Partition: " + metadata.partition() + "\n" +
									"Offset: " + metadata.offset() + "\n" +
									"Timestamp: " + metadata.timestamp());
					}else {
						logger.error("Error while producing message", exception);
					}
					
				}
			});
		}
		
		//flush data
		producer.flush();
		
		//flush and close
		producer.close();
	}
}
