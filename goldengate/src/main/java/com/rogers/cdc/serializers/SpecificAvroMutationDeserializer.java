package com.rogers.cdc.serializers;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;

import com.rogers.cdc.api.mutations.DeleteMutation;
import com.rogers.cdc.api.mutations.InsertMutation;
import com.rogers.cdc.api.mutations.Mutation;
import com.rogers.cdc.api.mutations.PkUpdateMutation;
import com.rogers.cdc.api.mutations.Row;
import com.rogers.cdc.api.mutations.RowMutation;
import com.rogers.cdc.api.mutations.UpdateMutation;
import com.rogers.cdc.api.schema.Table;
import com.rogers.cdc.api.serializer.MutationDeserializer;
import com.rogers.cdc.api.serializer.MutationSerializer;
import com.rogers.cdc.exceptions.SerializationException;
import com.rogers.cdc.kafka.KafkaUtil;

public class SpecificAvroMutationDeserializer extends AbstractSpecificAvroSerDe  implements MutationDeserializer{
	
	//private Deserializer<Object> deserializer;
	
	public SpecificAvroMutationDeserializer(){
	 }
	  public SpecificAvroMutationDeserializer(SchemaRegistryClient schemaRegistry){
		  super(schemaRegistry);
	  }
	
	@Override
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub
		super.configure(configs);
		
	}

	@Override
	public Mutation deserialize(String topic, byte[] payload) {
		// TODO Auto-generated method stub
		try { 
		   SchemaAndValue res = converter.toConnectData(topic, payload);

		   Schema schema = res.schema();
		   Object val = res.value();
		   Struct row = (Struct) val;
		   if (schema != null){
			   //TODO: There has to be a cleaner way of checking for Deletes 
			   // If null, it's a delete mutation. 
			   if (schema != row.schema()){
				   throw new SerializationException("Object schema doesn't match given schema");
			   }
		   }
		   
		   
		   return toMutation(topic, row);
         } catch (RuntimeException e) {
   
           throw new SerializationException("Error deserializing Avro message  ",  e);
        } 
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
	;
	 private Mutation toMutation(String topic, Struct struct){
		 
		   Row row = Row.fromStruct(struct);
		   String dbName =  KafkaUtil.topicToDbName(topic);
	       String tableName =  KafkaUtil.topicToTableName(topic);
		  
	       Schema schema = (struct == null) ? null :struct.schema();
		   Table table = new Table(dbName, tableName, schema, null);
		  
		    Mutation mutation; 
		   // TODO: Do a smarter job at detecting the mutation type
		    // PkUpdate: Check if all updated fields are the same like the key fields 
		    // Update: Check if any fields are null
		    // TODO: What to do about delete? We don't get schema info back from the converter. The best we can do is create a Table with a null schema. 
	    	if (row.size() == 0){
	        	    mutation =  new DeleteMutation(table);

	           }else {
	        	   mutation = new UpdateMutation(table, row);
	           }
	          
	    	return mutation;
	    }

}
