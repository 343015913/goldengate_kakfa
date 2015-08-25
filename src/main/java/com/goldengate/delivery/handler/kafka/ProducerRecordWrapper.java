package com.goldengate.delivery.handler.kafka;
import com.goldengate.delivery.handler.kafka.util.key.*;

import java.io.InputStream;
import java.util.Properties;

import com.goldengate.delivery.handler.kafka.util.EncryptedMessage;
import com.goldengate.delivery.handler.kafka.util.Encryptor;

import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// pseudo-typedef for ProducerRecord<K,V> (except that ProducerRecrod is final, so had to make up my own implementation without inheritance)
// Some consider it an anti-pattern, but entering the type information everywhere was just too painful
public class ProducerRecordWrapper {
	final private static Logger logger = LoggerFactory.getLogger(ProducerRecordWrapper.class);
	boolean encrypt = true;
	ProducerRecord<byte[], byte[]> rec;
	//TODO remove all the config stuff
	private final String KAFKA_CONFIG_FILE = "kafka.properties";
	private Properties config;
	Properties prop;
	InputStream inputStream;
	KeyProvider keyProvder; 
	private void initConfig(String propFileName) throws IOException{
	       config = new Properties();
			
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
			}
	    }
	public ProducerRecordWrapper(String topic, byte[] key, byte[] val) throws SerializationException {
		try {
		  initConfig(KAFKA_CONFIG_FILE);
		} catch (IOException e) {
		     
		}
		//keyProvder = new ConfigKeyProvider(config);
		try {
	    byte[] payload; 
		// TODO: Encryption is here temprorarly - create an Encrption serealizer
	      if (encrypt) {
		     //EncryptedMessage msg  = Encryptor.encrypt(val);
		     payload = val;
	      }else{
	    	 payload = val;
	      }
	      logger.info("Send message: " + payload) ;
	      logger.info("Send message: " +  Hex.encodeHexString(payload)) ;
	      logger.info("Send message: " +  new String(payload)) ;
	      
		  rec = new ProducerRecord<byte[], byte[]> (topic, key, payload);
		} catch (Exception e) {
		      // avro serialization can throw AvroRuntimeException, NullPointerException,
		      // ClassCastException, etc
			  //TODO Throw a diffrent expection
		      throw new SerializationException("Error serializing Avro message", e);
		 }
	}
	public ProducerRecordWrapper(String topic,  byte[] val){
		this(topic, null, val);
		
	}
	public ProducerRecord<byte[], byte[]> get(){
		return rec;
		
	}

}
