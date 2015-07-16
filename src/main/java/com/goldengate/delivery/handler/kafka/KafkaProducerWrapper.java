package com.goldengate.delivery.handler.kafka;


import java.io.IOException;
import java.io.InputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.lang.Byte;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

import com.goldengate.atg.datasource.GGDataSource.Status;
import com.goldengate.delivery.handler.kafka.ProducerRecordWrapper;


public class KafkaProducerWrapper {
	private Properties config;
	private InputStream inputStream;
	private String propFileName = "kafka.properties";
	private KafkaProducer<byte[],byte[]> producer;
	private boolean sync; 
	
	final private static Logger logger = LoggerFactory.getLogger(KafkaProducerWrapper.class);
	//TODO fix IOException nonsense
	public static void main(String [ ] args)
	{
		logger.info("Create KafkaProducerWrapper");
		KafkaProducerWrapper producer;
		try {
			   producer = new KafkaProducerWrapper();
		  
		     List<ProducerRecordWrapper> events = new ArrayList<ProducerRecordWrapper>();	;
		     ProducerRecordWrapper event = new ProducerRecordWrapper("test_topic", "test message".getBytes());
		     events.add(event);
		
			for (ProducerRecordWrapper rec: events){
				producer.send(rec);
			}
		} catch (Exception e1) {
			
			logger.error("Unable to deliver events " +  e1);
		}
		
		
		
	}
	public KafkaProducerWrapper() throws IOException{
		initConfig();
		 try {
			 producer = new KafkaProducer<byte[],byte[]>(getProducerProps());
			 Runtime.getRuntime().addShutdownHook(new Thread() {
				   @Override
				   public void run() {
					   producer.close();
				   }
				  });
	     }  catch (Exception e) {
		     System.out.println("Exception: " + e);
		     System.exit(1);
	     } 
  }
	//TODO async vs sync
	public void send(ProducerRecordWrapper record ) throws InterruptedException, ExecutionException {
		
		//ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]> (msg.topic, msg.key, msg.message);
		if (true || sync){
		   producer.send(record.get()).get();
		}
		else{
			producer.send(record.get());
			
		}
	}
	
	private Properties getProducerProps(){
		Properties props = new Properties();
		sync = Boolean.parseBoolean(config.getProperty("sync", "false"));
		// TODO Check for mandatory properties 
		// TODO add defaults 
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProperty("brokerList",  "52.4.197.159:9092"));
	  /*  props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.getProperty("compressionCodec"));
	    props.put(ProducerConfig.SEND_BUFFER_CONFIG, config.getProperty("socketBuffer"));
	    props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, config.getProperty("retryBackoffMs"));
	    props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, config.getProperty("metadataExpiryMs"));
	    props.put(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, config.getProperty("metadataFetchTimeoutMs"));
		props.put(ProducerConfig.ACKS_CONFIG, config.getProperty("requestRequiredAcks", "1"));
		props.put(ProducerConfig.TIMEOUT_CONFIG, config.getProperty("requestTimeoutMs"));
	    props.put(ProducerConfig.RETRIES_CONFIG, config.getProperty("messageSendMaxRetries"));
		props.put(ProducerConfig.LINGER_MS_CONFIG, config.getProperty("sendTimeout.toString"));
		if(Integer.parseInt(config.getProperty("queueEnqueueTimeoutMs")) != -1);
			props.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, "false");
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.getProperty("maxMemoryBytes.toString"));
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.getProperty("maxPartitionMemoryBytes.toString"));
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "goldengate-producer");
		*/
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

		return props;
	}
    private void initConfig() throws IOException{
       config = new Properties();
		/*
		try {
		  inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
		  if (inputStream != null) {
			config.load(inputStream);
		  } else {
			throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
		  }
		} catch (IOException e) {
			System.out.println("Exception: " + e);
		} finally {
			inputStream.close();
		}*/
    }
}
