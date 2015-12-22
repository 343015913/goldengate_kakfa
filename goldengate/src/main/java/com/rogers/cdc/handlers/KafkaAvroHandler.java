package com.rogers.cdc.handlers;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rogers.cdc.api.mutations.Mutation;
import com.rogers.cdc.api.mutations.MutationMapper;
import com.rogers.cdc.api.serializer.MutationSerializer;
import com.rogers.cdc.kafka.KafkaUtil;
import com.rogers.cdc.serializers.GenericAvroMutationSerializer;
import com.rogers.cdc.serializers.SpecificAvroMutationSerializer;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.serialization.Serializer;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;




// Accepts any representation of a CDC operation (Op) together with a Mapper that mappes Op to Mutation
public class KafkaAvroHandler<Op, Table, OpMapper extends MutationMapper<Op,  Table>> extends KafkaHandler<Op, Table, OpMapper> {	 
	 public  static  byte  PROTO_MAGIC_V0 = 0x0; 
	 
	final private static Logger logger = LoggerFactory
			.getLogger(KafkaAvroHandler.class);
	//TODO: Serilaizer should be a generic
	MutationSerializer valSerialiazer ;
	MutationSerializer keySerialiazer;
	//TODO: Add key serialiazer
    
	public KafkaAvroHandler(OpMapper _opMapper, String configFile) {
		  super(_opMapper, configFile);
		  //TODO: Config file? 
		  //valSerialiazer = new GenericAvroMutationSerializer();
		  valSerialiazer = new SpecificAvroMutationSerializer();
		  valSerialiazer.configure(new AbstractConfig(new ConfigDef(), config).originals(), false);
		  keySerialiazer = new SpecificAvroMutationSerializer();
		  keySerialiazer.configure(new AbstractConfig(new ConfigDef(), config).originals(), true);
		//  keySerialiazer = new KafkaAvroSerializer(); 
		  //keySerialiazer.configure(new AbstractConfig(new ConfigDef(), config).originals(), false);// TODO use AbstractKafkaAvroSerDeConfig when new confluent comes out
	  }
	 @Override
	 public  void processOp(Op op) {  
		 try { 
			   logger.debug("Start processing " , op);
               Mutation mutation = opMapper.toMutation(op);
               logger.debug("Mapped to mutation succesfully ");
               //TODO!!!! Rmove the if, it's just for testing
               if (mutation.getType() == com.rogers.cdc.api.mutations.MutationType.INSERT) {
            	  logger.debug("processes insert mutation " , op);
		          String topic = getSchemaSubject(mutation);
		          // byte[] key = keySerialiazer.serialize(mutation.);
                  byte[] val = valSerialiazer.serialize(topic, mutation);
                  byte[] key = keySerialiazer.serialize(topic, mutation);
                  logger.info("KafkaHandler: Send Message to topic: " + topic);
		          send(topic,key ,val);
               }
		      } catch  (IOException e)  {
			  //logger.error("KafkaAvroHandler Failed to processes operation: " + op + " with error: " + e );
			   throw new RuntimeException(" Failed to map op to Mutation ", e);
		    
	          } catch  (RuntimeException e)  {
	        	  logger.error("Failed to serialize or send mutation  with error: " + e );
	        	   throw new RuntimeException(" Failed to serialize or send mutation", e);
			  }
	    }
	
	
	 protected  String getSchemaSubject(Mutation op){
		 return KafkaUtil.genericTopic(op);
	 }

}
