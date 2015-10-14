package com.rogers.goldengate.serializers;

import java.io.File;
import java.io.IOException;



import java.io.Serializable;

import org.apache.avro.Schema;

import com.rogers.goldengate.api.mutations.Mutation;
import com.rogers.goldengate.avro.InsertMutation;

import java.lang.ClassLoader;
import java.net.URL;
import java.lang.Class;

abstract public class AbstructAvroSerDe implements  Serializable{
	 protected   static final  byte  PROTO_MAGIC_V0 = 0x0; 
	 protected static final int idSize = 2;
	 protected static final int opTypeSize = 1;
	 
			 
	 
	Schema schema = null; //TODO
	 
	AbstructAvroSerDe(){
		//TODO
			 try {
				// System.out.print(this.getClass().getResource("/"));
				 //URL url =  this.getClass().getResource("/com/rogers/goldengate/avro/mutations.avsc");
				 
				 
				  //schema = new Schema.Parser().parse(new File(url.getFile()));
				  schema =  InsertMutation.getClassSchema();
				}
				catch(Exception e){
					 throw new RuntimeException("Couldn't find Avro file" + e);
					
				}
	 }
	 // TODO: Need a real mock schemare registry
	//  INterface may depend on wheather we want to be able to evolve schemas,  
	 protected  Schema getSchema(Mutation op){
		 return schema; 
	 }
	 protected  Schema getSchemaById(short id){
		 return schema; 
	 }
	 protected  String getSchemaSubject(Mutation op){
		 return "test";  //TODO
	 }
	 protected  Short getSchemaId(String topic, Schema schema){
		 return 1; //TODO
	 };
}
