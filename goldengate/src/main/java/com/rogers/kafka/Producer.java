package com.rogers.kafka;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




import com.rogers.cdc.exceptions.ConfigException;

public class Producer{
	private Properties config;
	//private InputStream inputStream;
	private KafkaProducer<byte[], byte[]> producer;


	final private static Logger logger = LoggerFactory
			.getLogger(Producer.class);

	

	public Producer(String propFileName){

		logger.info("Kafka: Create KafkaProducerWrapper");
	
		initConfig(propFileName);

		try {
			producer = new KafkaProducer<byte[], byte[]>(config);
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					producer.close();
				}
			});
		
		} catch (Exception e) {
			logger.error("Failed to create KafkaProducer  " + e);
			//System.exit(1);
		}
		logger.info("Kafka: Done Create KafkaProducerWrapper");
	}

	public void send(String topic, byte[] key, byte[] msg) throws InterruptedException,
			ExecutionException {
		ProducerRecord<byte[], byte[]> rec = new ProducerRecord<byte[], byte[]>(topic, key, msg);
		producer.send(rec);

	}
	public void send(String topic,  byte[] msg) throws InterruptedException,
	ExecutionException {
       ProducerRecord<byte[], byte[]> rec = new ProducerRecord<byte[], byte[]>(topic,  msg);

       producer.send(rec);
    }
	private void initConfig(String propFileName){
		config = new Properties();

		try ( InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName)){	
			if (inputStream != null) {
				config.load(inputStream);
			} else {
				logger.error("Failed to find KafkaProducer config " + propFileName);
				throw new FileNotFoundException("property file '"
						+ propFileName + "' not found in the classpath");
			}
		} catch (IOException e) {
			logger.error("Failed to load KafkaProducer config " + propFileName + "with error " + e);
			throw new ConfigException("Failed to load KafkaProducer config"+ propFileName + "with error" + e);
		}
	}
}
