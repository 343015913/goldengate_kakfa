package com.goldengate.delivery.handler.kafka.util;

public class KeyedMessage<K,V> {
	public String topic;
	public K key;
	public K partKey;
	public V message;
	public KeyedMessage(String _topic, K _key, K _partKey, V _message ){
		topic = _topic;
		key = _key;
		partKey = _partKey;
		message = _message;
		if(topic == null)
		    throw new IllegalArgumentException("Topic cannot be null.");
	}
	public KeyedMessage(String topic, V message ){
		this(topic, null, null, message);
	}
	public KeyedMessage(String topic, K key, V message ){
		this(topic, key, key, message);
	}
	public K partitionKey() {
		    if(partKey != null)
		      return partKey;
		    else if(hasKey())
		      return key;
		    else
		      return null;  
		  }
	public boolean hasKey(){
		return key != null; 
	}
}
