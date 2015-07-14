package com.goldengate.delivery.handler.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
// pseudo-typedef for ProducerRecord<K,V> (except that ProducerRecrod is final, so had to make up my own implimentation without ingeritance)
// Some consider it an anti-pattern, but entering the type information everywhere was just too painful
public class ProducerRecordWrapper {
	ProducerRecord<byte[], byte[]> rec;
	public ProducerRecordWrapper(String topic, byte[] key, byte[] val){
		rec = new ProducerRecord<byte[], byte[]> (topic, key, val);
	}
	public ProducerRecordWrapper(String topic,  byte[] val){
		rec = new ProducerRecord<byte[], byte[]> (topic, val);
	}
	public ProducerRecord<byte[], byte[]> get(){
		return rec;
		
	}

}
