package com.rogers.cdc.serializers;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rogers.cdc.api.mutations.*;
import com.rogers.cdc.exceptions.InvalidTypeException;
import com.rogers.cdc.exceptions.SerializationException;


public class GenericAvroMutationSerializer extends AbstractAvroMutationSerialzer  {
 
	final private static Logger logger = LoggerFactory
			.getLogger(GenericAvroMutationSerializer.class);
	
	

	public GenericAvroMutationSerializer() {
		 // super(configFile);
		
	  }
	 @Override
	public void configure(Map<String, ?> configs){
		
	}
	 @Override
		public void close(){
			
		}
	
	 // TODO: The switch statment is ugly. Move it to a helper OpProc class, with a subclass for each type
	    private  void addBody(GenericRecord record, Mutation op){
	        switch(op.getType()){
	           case INSERT:
	           {
	        	   InsertMutation mutation =  op.getMutation();
	        	   this.processInsertOp(mutation,record);
	        	   break;
	           }
	           case  DELETE: {
	        	  
	        	    this.processDeleteOp(op,record);
	        	   break;
	           }
	           case UPDATE: 
	           {
	        	   UpdateMutation mutation =  op.getMutation();
	        	    this.processUpdateOp(mutation,record);
	        	   break;
	           }
	           case PKUPDATE: 
	        	    this.processPkUpdateOp(op,record);
	        	   break;
	           default:
	        	   logger.error("The operation type " + op.getType() + " on  operation: table=[" + op.getTableName() + "]" + "is not supported");
	        	   throw new IllegalArgumentException("KafkaAvroHandler::addBody Unknown operation type");                                                                            
          }                                                                                              
	    }
	 

	 protected  GenericData.Record avroRecord(Mutation op, Schema schema){
		    GenericData.Record record = new GenericData.Record(schema);
			addHeader(record, op);
			addBody(record,op);
			return record; 
	  }
	protected void processPkUpdateOp(Mutation op, GenericRecord record) {
		// TODO Auto-generated method stub

	}

	
	protected void processUpdateOp(UpdateMutation op, GenericRecord record) {

		  Map<String,String> strings = new HashMap<String,String>();
		  int i = 0;
		     for(Map.Entry<String,Column> column : op.getRow().getColumns().entrySet()) {  
		    	   String name = column.getKey(); 
	    		   Object val = column.getValue().getValue();
		    	     try{		
		    		     strings.put(name,(String)val);
		    	     } catch (ClassCastException e) {
		    	         // With collections, the element type has not been checked, so it can throw
		    	          throw new InvalidTypeException("Invalid column type, expeting a String: " + e.getMessage());
		    	     }
		     } 
		   record.put("strings", strings);

	}

	
	protected void processDeleteOp(Mutation op, GenericRecord record) {
		// Nothing to do
		Map<String,String> strings = new HashMap<String,String>();
		record.put("strings", strings);

	}
	protected void processInsertOp(InsertMutation op, GenericRecord record) {
		  Map<String,String> strings = new HashMap<String,String>();
		  int i = 0;
		     for(Map.Entry<String,Column> column : op.getRow().getColumns().entrySet()) {  
		    	   String name = column.getKey(); 
		    	   Object val = column.getValue().getValue();
		    	     try{		
		    		     strings.put(name,(String)val);
		    	     } catch (ClassCastException e) {
		    	         // With collections, the element type has not been checked, so it can throw
		    	          throw new InvalidTypeException("Invalid column type, expeting a String: " + e.getMessage());
		    	     }
		     } 
		   record.put("strings", strings);

	}
	

	@Override
	  protected  byte[] serializeAvro( GenericData.Record record,  Schema schema, String topic,  byte opType) throws IOException {
		        short schemaId = getSchemaId(topic, schema);
			    EncoderFactory encoderFactory = EncoderFactory.get();
			    DatumWriter<GenericRecord> writer  = new GenericDatumWriter<GenericRecord>();
			    writer.setSchema(schema);
			    ByteArrayOutputStream out = new ByteArrayOutputStream();
			    out.write(PROTO_MAGIC_V0);
			    out.write(ByteBuffer.allocate(opTypeSize).put(opType).array() );
			    out.write(ByteBuffer.allocate(idSize).putShort(schemaId).array());
			    BinaryEncoder enc = encoderFactory.binaryEncoder(out, null);
			    writer.write(record, enc);
			    enc.flush();
			    return out.toByteArray();
	  }
	
}
