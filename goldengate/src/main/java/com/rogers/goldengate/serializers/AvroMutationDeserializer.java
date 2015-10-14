package com.rogers.goldengate.serializers;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rogers.goldengate.api.mutations.Column;
import com.rogers.goldengate.api.mutations.DeleteMutation;
import com.rogers.goldengate.api.mutations.InsertMutation;
import com.rogers.goldengate.api.mutations.Mutation;
import com.rogers.goldengate.api.mutations.MutationType;
import com.rogers.goldengate.api.mutations.PkUpdateMutation;
import com.rogers.goldengate.api.mutations.Row;
import com.rogers.goldengate.api.mutations.UpdateMutation;
import com.rogers.goldengate.exceptions.SerializationException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;



public class AvroMutationDeserializer extends AbstractAvroDeserilaizer implements MutationDeserializer , Serializable {
	
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
