package com.rogers.cdc.serializers;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import java.util.Map;

import org.apache.avro.Schema;

import com.rogers.cdc.api.mutations.Mutation;


abstract public class AbstractSpecificAvroSerDe{
	protected AvroConverter converter; 
	private static final int SCHEMAS_CACHE_SIZE_DEFAULT = 1000;
	protected AvroData avroData;
	 
	AbstractSpecificAvroSerDe(){
		converter = new AvroConverter();
		avroData = new AvroData(SCHEMAS_CACHE_SIZE_DEFAULT);
	 }
	AbstractSpecificAvroSerDe(SchemaRegistryClient schemaRegistry ){
		converter = new AvroConverter(schemaRegistry);
		avroData = new AvroData(SCHEMAS_CACHE_SIZE_DEFAULT);
	 }
	public void configure(Map<String, ?> configs) {
		//converter.configure(configs, isKey);
		converter.configure(configs, false);
	}
	 // TODO: Need a real mock schemare registry
	//  INterface may depend on wheather we want to be able to evolve schemas,  
	 //protected  Schema getSchema(Mutation op){
		// return avroData.fromConnectSchema(op.getTable().getSchema()); 
	 //}
	/* protected  Schema getSchemaById(short id){
		 return schema; 
	 }
	 protected  String getSchemaSubject(Mutation op){
		 return "test";  //TODO
	 }
	 protected  Short getSchemaId(String topic, Schema schema){
		 return 1; //TODO
	 };*/
}