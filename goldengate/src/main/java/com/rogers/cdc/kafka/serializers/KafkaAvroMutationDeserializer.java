package com.rogers.cdc.kafka.serializers;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import com.rogers.cdc.api.mutations.Mutation;
import com.rogers.cdc.serializers.GenericAvroMutationDeserializer;
/*Can be plugged into Kafka config as a desirilaizer */
public class KafkaAvroMutationDeserializer extends GenericAvroMutationDeserializer
		implements Deserializer<Mutation> {
	 private Deserializer<byte[]> firstDeserializer;
	 KafkaAvroMutationDeserializer(Deserializer<byte[]> _firstDeserializer){
		 if(_firstDeserializer == null){
			 firstDeserializer = new ByteArrayDeserializer();
			 //TODO: Should get this from config - look at KafkaProducer code...(in Kafka src)
		 }else{
			 firstDeserializer = _firstDeserializer;
		 }

	 }
	 @Override
	  public void configure(Map<String, ?> configs, boolean isKey) {
	  }

	  @Override
	  public Mutation deserialize(String topic, byte[] data) {
		byte[] bytes; 
		if (firstDeserializer != null){
			bytes = firstDeserializer.deserialize(topic, data);
		}else{
			bytes = data;
		}
	    return deserializeImp(bytes);
	  }

	  @Override
	  public void close() {

	  }

	
}
