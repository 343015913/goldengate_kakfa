package com.rogers.cdc.serializers;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.rogers.cdc.api.mutations.Mutation;
import com.rogers.cdc.exceptions.SerializationException;


// TODO:There are some major inconsistencies between the Generic and Specific serializer 
//1) Representation of Update/Insert/Delete Mutations: The Specific serializer uses the same schema for all 3 - if the val is empty, it's a delete Mutation 
//2) The Specific serializer uses Confluent.io serializer, the Generic one does it's own serialization
//3) Diffrent headers
public abstract class AbstractAvroMutationSerialzer extends AbstructAvroSerDe implements MutationSerializer{
	protected void addHeader(GenericRecord record, Mutation op) {
		String tableName = op.getTableName();
	    String schemaName = op.getSchemaName();
	    record.put("table", tableName);
	    record.put("schema", schemaName);
	  }
	abstract protected  GenericData.Record avroRecord(Mutation op, Schema schema);
	abstract protected  byte[] serializeAvro( GenericData.Record record,  Schema schema, String topic,  byte opType) throws IOException ;
	
	@Override
    public byte[] serialize(Mutation op) {  
	 // TODO topic shouldn't be handled here
     String topic = getSchemaSubject(op);
	 Schema schema = getSchema(op);
	 byte opType = op.getMagicByte();
	
	 GenericData.Record record = avroRecord(op, schema);
	 byte[] bytes;
	 try{ 
		 bytes = serializeAvro(record, schema, topic, opType);
	 }catch (IOException e){
		 throw new SerializationException("Failed to serialze Avro object, with error: " + e);
	 }
     return bytes; 

	}
}
