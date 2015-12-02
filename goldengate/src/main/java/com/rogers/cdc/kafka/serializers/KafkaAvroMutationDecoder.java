package com.rogers.cdc.kafka.serializers;

import java.util.Map;

import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

import com.rogers.cdc.api.mutations.Mutation;
import com.rogers.cdc.serializers.AbstractAvroDeserilaizer;

import kafka.utils.VerifiableProperties;


public class KafkaAvroMutationDecoder extends AbstractAvroDeserilaizer
implements Decoder<Mutation> {
private Decoder<byte[]> firstDeserializer;
/**
 * Constructor used for testing.
 */
public KafkaAvroMutationDecoder(Decoder<byte[]> _firstDeserializer){
	//TODO: Move to AbstractAvroDeserilaizer
 if(_firstDeserializer == null){
	 firstDeserializer = new DefaultDecoder(null);
	 //TODO: Should get this from config - look at KafkaProducer code...(in Kafka src)
 }else{
	 firstDeserializer = _firstDeserializer;
 }

}
/**
 * Constructor used by Kafka consumer.
 */
public KafkaAvroMutationDecoder(VerifiableProperties props) {
	//TODO Parase Props file....
	 firstDeserializer = new DefaultDecoder(null);
}


public void configure(VerifiableProperties props) {
}


@Override
public Mutation fromBytes( byte[] data) {
byte[] bytes; 
if (firstDeserializer != null){
	bytes = firstDeserializer.fromBytes( data);
}else{
	bytes = data;
}
return deserializeImp(bytes);
}




}

