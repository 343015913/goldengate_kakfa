package com.rogers.cdc.serializers;

import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rogers.cdc.api.mutations.Mutation;
import com.rogers.cdc.api.mutations.RowMutation;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
//import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
//import io.confluent.kafka.serializers.NonRecordContainer;

public class SpecificAvroMutationSerializer extends AbstractSpecificAvroSerDe implements MutationSerializer{ 
	final private static Logger logger = LoggerFactory
			.getLogger(SpecificAvroMutationSerializer.class);
	  private Serializer<Object> serializer;
	
	 // public static final String SCHEMAS_CACHE_SIZE_CONFIG = "schemas.cache.config";
	  //private static final int SCHEMAS_CACHE_SIZE_DEFAULT = 1000;
	
	 // private SchemaRegistryClient schemaRegistry;
	  
	  public SpecificAvroMutationSerializer(){
		  serializer = new KafkaAvroSerializer();
	  }
		@Override
		public void configure(Map<String, ?> configs) {
			//serializer = new KafkaAvroSerializer();
			//serializer.configure(configs, false); // This usually gets called by Kafka...but we have to call it here since we never pass the serialzer to Kafk	
			
			converter.configure(configs, false);
		}
	  @Override
		 public byte[] serialize(String topic, Mutation op) {  
			 // TODO topic shouldn't be handled here
		     //String topic = getSchemaSubject(op);
		  
			// Schema schema = getSchema(op);
			// byte opType = op.getMagicByte();
			 //logger.debug("Try to serialize: topic = {}, \n mutation = {}, \n schema = {}  ", topic, op, schema);
			
			 Struct record = getRecord(op);
			// byte[] bytes;
      	    // logger.debug("\t recrod = {}  ", record);

			 try{ 
				 //bytes = serializer.serialize(topic, record);
				 /*EncoderFactory encoderFactory = EncoderFactory.get();
				 ByteArrayOutputStream out = new ByteArrayOutputStream();
				 logger.debug("1");
				 BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
				 logger.debug("2");
			        DatumWriter<Object> writer;
			        Object value = record ;
			        //if (value instanceof SpecificRecord) {
			          //writer = new SpecificDatumWriter<Object>(schema);
			        //} else {
			          writer = new GenericDatumWriter<Object>(schema);
			          logger.debug("3");
			       // }
			        writer.write(value, encoder);
			        logger.debug("4");
			        encoder.flush();
			        logger.debug("5");
			        byte[] bytes2 = out.toByteArray(); 
			        out.close();
			        logger.debug("6");*/
				 logger.debug("schema = " + record.schema());
				 logger.debug("recrod = " + record);

				    Object obj = avroData.fromConnectData(record.schema(), record);
				    logger.debug("obj = " + obj);
				    return serializer.serialize(topic,obj);
			      //  bytes = converter.fromConnectData(topic, record.schema(), record);
			 }catch (Exception e){
	        	   logger.error("Confluent KafkaAvroSerializer serialization error: " + e);

				 throw new SerializationException("Failed to serialze Avro object, with error: " + e);
			 }
		     //return bytes; 

		 }
	
	 protected  Struct getRecord(Mutation op){
		    Struct record;
		    logger.debug("\t avroRecord()  ");
		    
	        switch(op.getType()){
	           case INSERT:
	           case UPDATE:
	           {
	        	   logger.debug("\t Insert/Update  ");
	        	   RowMutation mutation =  op.getMutation();
	        	 //  this.processRowOp(mutation,record);
	        	   record = mutation.getRow().toStruct(mutation.getTable().getSchema());
	        	   break;
	        	  // break;
	           }
	           case  DELETE: {  
	        	   logger.debug("\t Delete  ");
	        	   //Nothing is sent for delete
	        	   record = null; 
	        	   break;
	           }         
	           case PKUPDATE:
	        	   logger.debug("\t PKUPDATE ");
	        	   // Just writing the new Pkey value. The only value is sent in the key. 
	        	   RowMutation mutation =  op.getMutation();
	        	   record = mutation.getRow().toStruct(mutation.getTable().getSchema());
	        	 //  this.processRowOp(mutation,record);
	        	  
	        	   break;
	        	  /// logger.error("The operation type PKUPDATE on table=[" + op.getTableName() + "]" + "is not supported");
	        	  // throw new IllegalArgumentException("KafkaAvroHandler::addBody PKUPDATE operation not supported");   
	           default:
	        	   logger.error("The operation type " + op.getType() + " on  operation: table=[" + op.getTableName() + "]" + "is not supported");
	        	   throw new IllegalArgumentException("KafkaAvroHandler::addBody Unknown operation type");                                                                            
       }  
	        return record;
	    }

	/*	protected void processRowOp(RowMutation op, GenericRecord record) {
			     for(Map.Entry<String,Column> column : op.getRow().getColumns().entrySet()) {  
			    	   String name = column.getKey(); 
			    	   Object val = column.getValue().getValue();
			    	     try{		
			    	    	 record.put(name, val);
			    	     } catch (ClassCastException e) {
			    	          throw new InvalidTypeException("Invalid column type: " + e.getMessage());
			    	     }
			     } 
		}*/



	@Override
	public void close() {
		

	}

}
