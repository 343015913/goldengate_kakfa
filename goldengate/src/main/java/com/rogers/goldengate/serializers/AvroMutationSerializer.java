package com.rogers.goldengate.serializers;

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

import com.rogers.goldengate.api.mutations.Column;
import com.rogers.goldengate.api.mutations.InsertMutation;
import com.rogers.goldengate.api.mutations.Mutation;
import com.rogers.goldengate.api.mutations.UpdateMutation;
import com.rogers.goldengate.exceptions.InvalidTypeException;
import com.rogers.goldengate.handlers.KafkaAvroHandler;
import com.rogers.goldengate.exceptions.SerializationException;
import com.rogers.goldengate.exceptions.InvalidTypeException;

public class AvroMutationSerializer extends AbstructAvroSerDe implements MutationSerializer {
 
	 
	
	 
	final private static Logger logger = LoggerFactory
			.getLogger(AvroMutationSerializer.class);
	
	

	public AvroMutationSerializer() {
		 // super(configFile);
		
	  }
	 @Override
	public void configure(Map<String, ?> configs){
		
	}
	 @Override
		public void close(){
			
		}
	 @Override
	    public byte[] serialize(Mutation op) {  
		 // TODO topic shouldn't be handled here
         String topic = getSchemaSubject(op);
		 Schema schema = getSchema(op);
		 byte opType = op.getMagicByte();
		 short schemaId = getSchemaId(topic, schema);
		 GenericData.Record record = avroRecord(op, schema);
		 byte[] bytes;
		 try{ 
			 bytes = serialize(record, schema, schemaId, opType);
		 }catch (IOException e){
			 throw new SerializationException("Failed to serialze Avro object, with error: " + e);
		 }
         return bytes; 
		 //send(topic, bytes);                                                                                    
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
		// TODO Auto-generated method stub
		  Map<String,String> strings = new HashMap<String,String>();
		  int i = 0;
		     for(Map.Entry<String,Column> column : op.getRow().getColumns().entrySet()) {  
		    	   String name = column.getKey(); 
	    		   Object val = column.getValue().getValue();
		    	     try{		
		    		     strings.put(name,(String)val);
		    	     } catch (ClassCastException e) {
		    	         // With collections, the element type has not been checked, so it can throw
		    	          throw new InvalidTypeException("Invalid type for collection element: " + e.getMessage());
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
		    	          throw new InvalidTypeException("Invalid type for collection element: " + e.getMessage());
		    	     }
		     } 
		   record.put("strings", strings);

	}
	
	private void addHeader(GenericRecord record, Mutation op) {
		String tableName = op.getTableName();
	    String schemaName = op.getSchemaName();
	    record.put("table", tableName);
	    record.put("schema", schemaName);
	  }
	  
	  private  byte[] serialize( GenericData.Record record,  Schema schema, short schemaId,  byte opType) throws IOException {
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
