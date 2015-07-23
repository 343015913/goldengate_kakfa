package com.goldengate.delivery.handler.kafka;


import com.goldengate.delivery.handler.kafka.util.EncryptedMessage;
import com.goldengate.delivery.handler.kafka.util.Encryptor;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayOutputStream;

import org.apache.kafka.clients.producer.ProducerRecord;
// pseudo-typedef for ProducerRecord<K,V> (except that ProducerRecrod is final, so had to make up my own implimentation without ingeritance)
// Some consider it an anti-pattern, but entering the type information everywhere was just too painful
public class ProducerRecordWrapper {
	boolean encrypt = true;
	ProducerRecord<byte[], byte[]> rec;
	public ProducerRecordWrapper(String topic, byte[] key, byte[] val) throws SerializationException {
		try {
	    byte[] payload; 
		// TODO: Encryption is here temprorarly - create an Encrption serealizer
	      if (encrypt) {
		     EncryptedMessage msg  = Encryptor.encrypt(val);
		     payload = msg.toByteArray();
	      }else{
	    	 payload = val;
	      }
		  rec = new ProducerRecord<byte[], byte[]> (topic, key, payload);
		} catch (Exception e) {
		      // avro serialization can throw AvroRuntimeException, NullPointerException,
		      // ClassCastException, etc
			  //TODO Throw a diffrent expection
		      throw new SerializationException("Error serializing Avro message", e);
		 }
	}
	public ProducerRecordWrapper(String topic,  byte[] val){
		rec = new ProducerRecord<byte[], byte[]> (topic, val);
	}
	public ProducerRecord<byte[], byte[]> get(){
		return rec;
		
	}

}
