package com.rogers.goldengate.handlers;

import java.io.IOException;
import java.io.File;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

/*
import com.goldengate.atg.datasource.DsColumn;
import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.atg.datasource.meta.TableName;
import com.goldengate.delivery.handler.kafka.HandlerProperties;
*/


import com.rogers.goldengate.api.mutations.*;
import com.rogers.goldengate.exceptions.*;
import com.rogers.goldengate.kafka.KafkaUtil;
import com.rogers.goldengate.serializers.*;
import com.rogers.kafka.Producer;


//TODO: This class should really be KafkaHander<AvroSeriailzer>
public class KafkaAvroHandler extends KafkaHandler {
	// TODO: Move to an Op wrapper
	 public static byte UnknownByte = 0x0;
	 public static byte InsertByte = 0x1;
	 public static byte UpdateByte = 0x2;
	 public static byte DeleteByte = 0x3;
	 public static byte UpdatePKByte = 0x4; 
	 
	 public  static  byte  PROTO_MAGIC_V0 = 0x0; 
	 
	final private static Logger logger = LoggerFactory
			.getLogger(KafkaAvroHandler.class);
	//TODO: Serilaizer should be a generic
	MutationSerializer serialiazer ;

	public KafkaAvroHandler(String configFile) {
		  super(configFile);
		  serialiazer = new AvroMutationSerializer();
	  }
	 @Override
	 public  void processOp(Mutation op) {                                                                                              
        
		 String topic = getSchemaSubject(op);
         byte[] bytes = serialiazer.serialize(op);
         logger.info("KafkaHandler: Send Message to topic: " + topic);
         logger.info("\t message =  " + bytes);
         logger.info("\t string message =  " + new String(bytes));
		 send(topic, bytes);                                                                                    
	    }
	
	
	 protected  String getSchemaSubject(Mutation op){
		 return KafkaUtil.genericTopic(op);
	 }

}
