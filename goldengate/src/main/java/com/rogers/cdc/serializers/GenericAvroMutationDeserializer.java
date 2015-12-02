package com.rogers.cdc.serializers;

import java.io.Serializable;
import java.util.Map;


import com.rogers.cdc.api.mutations.Mutation;





public class GenericAvroMutationDeserializer extends AbstractAvroDeserilaizer implements MutationDeserializer , Serializable {
	
	@Override
	 public void configure(Map<String, ?> configs){
		 
	 }
	 @Override
	 public Mutation deserialize( byte[]  payload){
		return  deserializeImp(payload);
	//	 Mutation mutation = Mutation.fromAvroRec(rec, type);
		 
	 }
     @Override
	    public void close(){
	 }
	    
	   
}
