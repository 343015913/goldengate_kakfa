package com.goldengate.delivery.handler.kafka;


import java.io.IOException;
import java.io.InputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.lang.Byte;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import org.apache.commons.codec.binary.Hex;
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
	private String propFileName = "kafka.properties";
	private KafkaProducer<byte[],byte[]> producer;
	//private  ConsumerConnector consumer;
	private boolean sync; 
	private boolean encrypt; 
	
	final private static Logger logger = LoggerFactory.getLogger(KafkaProducerWrapper.class);
	//TODO fix IOException nonsense
	public static void main(String [ ] args)
	{
		String topic = "test_topic13";
		KafkaProducerWrapper producer;
		  ConsumerConnector consumer;
		try {
			   producer = new KafkaProducerWrapper();
		  
		     List<ProducerRecordWrapper> events = new ArrayList<ProducerRecordWrapper>();	;
		     ProducerRecordWrapper event = new ProducerRecordWrapper(topic, "test message".getBytes());
		     for (int i = 0; i < 10; i++){
		         events.add(event);
		     }
		
			for (ProducerRecordWrapper rec: events){
				producer.send(rec);
			}
			System.out.println(event);
			Thread.sleep(4000);
			Properties props = new Properties();
		     props.put("metadata.broker.list", "52.4.197.159:9092");
		     props.put("zookeeper.connect", "52.4.197.159:2181");
		     props.put("group.id", "test6");
		     props.put("autooffset.reset", "smallest");
		     props.put("enable.auto.commit", "true");
		     props.put("auto.commit.interval.ms", "1000");
		     props.put("session.timeout.ms", "30000");
		     /*
		     props.put("key.serializer", "org.apache.kafka.common.serializers.ByteArraySerializer");
		     props.put("value.serializer", "org.apache.kafka.common.serializers.ByteArraySerializer");
		     props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		     props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");*/
		     props.put("key.serializer", "org.apache.kafka.common.serializers.StringSerializer");
		     props.put("value.serializer", "org.apache.kafka.common.serializers.StringSerializer");
		     /*props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");*/
		     props.put("partition.assignment.strategy", "roundrobin");
		     consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props))	;
		//consumer.subscribe("test_topic3");
		
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
	    topicCountMap.put(topic, 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        KafkaStream<byte[], byte[]> m_stream = streams.get(0);
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext())
            System.out.println("Consumer message "  + new String(Hex.encodeHexString(it.next().message())));
        //System.out.println("Shutting down Thread: " + m_threadNumber);
	    
         } catch (Exception e1) {
			
			logger.error("Unable to deliver events " +  e1);
		}
		
		
	}
	public KafkaProducerWrapper() throws IOException{
		logger.info("Kafka: Create KafkaProducerWrapper");
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
		 logger.info("Kafka: Done Create KafkaProducerWrapper");
  }
	//TODO async vs sync
	public void send(ProducerRecordWrapper record ) throws InterruptedException, ExecutionException {
		
		logger.info("Kafka: KafkaProducerWrapper:Send");
		byte[] bytes; 
		/*if (encrypt){
			
			
		}else{
			bytes = record.get(); 
		}*/
		//ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]> (msg.topic, msg.key, msg.message);
		if ( sync){
		   producer.send(record.get()).get();
		}
		else{
			producer.send(record.get());
			
		}
	}
	
	public Properties getProducerProps(){
		Properties props = new Properties();
		sync = Boolean.parseBoolean(config.getProperty("sync", "false"));
		encrypt = Boolean.parseBoolean(config.getProperty("encryption", "true"));
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
