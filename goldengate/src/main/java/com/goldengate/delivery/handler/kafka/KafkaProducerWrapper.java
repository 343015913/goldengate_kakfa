package com.goldengate.delivery.handler.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.lang.Byte;

/*
 import kafka.consumer.ConsumerConfig;
 import kafka.consumer.KafkaStream;
 import kafka.javaapi.consumer.ConsumerConnector;
 import kafka.consumer.ConsumerIterator;
 import kafka.consumer.KafkaStream;*/

//import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.concurrent.ExecutionException;

import com.goldengate.atg.datasource.GGDataSource.Status;
import com.goldengate.delivery.handler.kafka.ProducerRecordWrapper;

public class KafkaProducerWrapper {
	private Properties config;
	private InputStream inputStream;
	private KafkaProducer<byte[], byte[]> producer;
	private boolean sync;
	private boolean encrypt;

	final private static Logger logger = LoggerFactory
			.getLogger(KafkaProducerWrapper.class);

	// TODO fix IOException nonsense

	public KafkaProducerWrapper(String propFileName) throws IOException {

		logger.info("Kafka: Create KafkaProducerWrapper");
		initConfig(propFileName);
		// ProducerConfig prodConfig = new ProducerConfig(config);
		try {
			producer = new KafkaProducer<byte[], byte[]>(config);
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					producer.close();
				}
			});
		} catch (Exception e) {
			System.out.println("Exception: " + e);
			System.exit(1);
		}
		logger.info("Kafka: Done Create KafkaProducerWrapper");
	}

	public void send(ProducerRecordWrapper record) throws InterruptedException,
			ExecutionException {

		logger.info("Kafka: KafkaProducerWrapper:Send");
		// byte[] bytes;

		// if ( sync){
		// producer.send(record.get()).get();
		// }
		// else{
		producer.send(record.get());

		// }
	}

	/*
	 * public Properties getProducerProps(){ Properties props = new
	 * Properties(); sync = Boolean.parseBoolean(config.getProperty("sync",
	 * "false")); encrypt =
	 * Boolean.parseBoolean(config.getProperty("encryption", "true")); // TODO
	 * Check for mandatory properties // TODO add defaults
	 * props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
	 * config.getProperty("brokerList", "52.4.197.159:9092"));
	 * props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
	 * config.getProperty("compressionCodec"));
	 * props.put(ProducerConfig.SEND_BUFFER_CONFIG,
	 * config.getProperty("socketBuffer"));
	 * props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,
	 * config.getProperty("retryBackoffMs"));
	 * props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG,
	 * config.getProperty("metadataExpiryMs"));
	 * props.put(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG,
	 * config.getProperty("metadataFetchTimeoutMs"));
	 * props.put(ProducerConfig.ACKS_CONFIG,
	 * config.getProperty("requestRequiredAcks", "1"));
	 * props.put(ProducerConfig.TIMEOUT_CONFIG,
	 * config.getProperty("requestTimeoutMs"));
	 * props.put(ProducerConfig.RETRIES_CONFIG,
	 * config.getProperty("messageSendMaxRetries"));
	 * props.put(ProducerConfig.LINGER_MS_CONFIG,
	 * config.getProperty("sendTimeout.toString"));
	 * if(Integer.parseInt(config.getProperty("queueEnqueueTimeoutMs")) != -1);
	 * props.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, "false");
	 * props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,
	 * config.getProperty("maxMemoryBytes.toString"));
	 * props.put(ProducerConfig.BATCH_SIZE_CONFIG,
	 * config.getProperty("maxPartitionMemoryBytes.toString"));
	 * props.put(ProducerConfig.CLIENT_ID_CONFIG, "goldengate-producer");
	 * 
	 * //props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
	 * "org.apache.kafka.common.serialization.ByteArraySerializer");
	 * //props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
	 * "org.apache.kafka.common.serialization.ByteArraySerializer");
	 * 
	 * return props; }
	 */
	private void initConfig(String propFileName) throws IOException {
		config = new Properties();

		try {
			inputStream = getClass().getClassLoader().getResourceAsStream(
					propFileName);
			if (inputStream != null) {
				config.load(inputStream);
			} else {
				throw new FileNotFoundException("property file '"
						+ propFileName + "' not found in the classpath");
			}
		} catch (IOException e) {
			System.out.println("Exception: " + e);
		} finally {
			inputStream.close();
		}
	}
}
