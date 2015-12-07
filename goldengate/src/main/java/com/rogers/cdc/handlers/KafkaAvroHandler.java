package com.rogers.cdc.handlers;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rogers.cdc.api.mutations.Mutation;
import com.rogers.cdc.api.mutations.MutationMapper;
import com.rogers.cdc.kafka.KafkaUtil;
import com.rogers.cdc.serializers.GenericAvroMutationSerializer;
import com.rogers.cdc.serializers.MutationSerializer;
import com.rogers.cdc.serializers.SpecificAvroMutationSerializer;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.serialization.Serializer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;




// Accepts any representation of a CDC operation (Op) together with a Mapper that mappes Op to Mutation
public class KafkaAvroHandler<Op, Table, OpMapper extends MutationMapper<Op,  Table>> extends KafkaHandler<Op, Table, OpMapper> {	 
	 public  static  byte  PROTO_MAGIC_V0 = 0x0; 
	 
	final private static Logger logger = LoggerFactory
			.getLogger(KafkaAvroHandler.class);
	//TODO: Serilaizer should be a generic
	MutationSerializer valSerialiazer ;
	Serializer<Object> keySerialiazer;
	//TODO: Add key serialiazer
    
	public KafkaAvroHandler(OpMapper _opMapper, String configFile) {
		  super(_opMapper, configFile);
		  //TODO: Config file? 
		  //valSerialiazer = new GenericAvroMutationSerializer();
		  valSerialiazer = new SpecificAvroMutationSerializer();
		  keySerialiazer = new KafkaAvroSerializer(); 
		  keySerialiazer.configure(new AbstractConfig(new ConfigDef(), config).originals(), false);// TODO use AbstractKafkaAvroSerDeConfig when new confluent comes out
	  }
	 @Override
	 public  void processOp(Op op) {  
		 try { 
               Mutation mutation = opMapper.toMutation(op);

		       String topic = getSchemaSubject(mutation);
		      // byte[] key = keySerialiazer.serialize(mutation.);
               byte[] val = valSerialiazer.serialize(topic, mutation);
               logger.info("KafkaHandler: Send Message to topic: " + topic);
               logger.debug("\t message =  " + val);
               logger.debug("\t string message =  " + new String(val));
		       send(topic,null ,val);    
		    } catch  (IOException e)  {
			   logger.error("KafkaAvroHandler Failed to processes operation: " + op + " with error: " + e ); 
		    }
	    }
	
	
	 protected  String getSchemaSubject(Mutation op){
		 return KafkaUtil.genericTopic(op);
	 }

}
