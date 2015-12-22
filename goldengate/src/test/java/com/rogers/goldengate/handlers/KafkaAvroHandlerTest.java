package com.rogers.goldengate.handlers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.DefaultDecoder;
import kafka.utils.VerifiableProperties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
//import org.junit.Test;






import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import com.rogers.cdc.api.mutations.Column;
import com.rogers.cdc.api.mutations.DeleteMutation;
import com.rogers.cdc.api.mutations.InsertMutation;
import com.rogers.cdc.api.mutations.Mutation;
import com.rogers.cdc.api.mutations.PassThroughMutationMapper;
import com.rogers.cdc.api.mutations.Row;
import com.rogers.cdc.api.mutations.UpdateMutation;
import com.rogers.cdc.api.schema.Table;
import com.rogers.cdc.handlers.Handler;
import com.rogers.cdc.handlers.KafkaAvroHandler;
import com.rogers.cdc.kafka.KafkaUtil;
import com.rogers.cdc.kafka.consumers.KafkaMutationAvroConsumer;



public class KafkaAvroHandlerTest {
	private Properties config;

	InputStream inputStream;
	private final String KAFKA_CONFIG_FILE = "kafka.properties";
	
	private KafkaMutationAvroConsumer consumer;
	
	private static final Schema FLAT_STRUCT_SCHEMA = SchemaBuilder.struct()
            .field("int8", Schema.INT8_SCHEMA)
            .field("int16", Schema.INT16_SCHEMA)
            .field("int32", Schema.INT32_SCHEMA)
            .field("int64", Schema.INT64_SCHEMA)
            .field("float32", Schema.FLOAT32_SCHEMA)
            .field("float64", Schema.FLOAT64_SCHEMA)
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .field("string", Schema.STRING_SCHEMA)
            .field("bytes", Schema.BYTES_SCHEMA)
            .build();
	//com.rogers.cdc.api.schema.Table table = new Table("testSchema", "testTable");
	//table.setSchema(FLAT_STRUCT_SCHEMA);
	Table table = new Table("testSchema", "testTable");
	//table.setSchema(FLAT_STRUCT_SCHEMA, Arrays.asList("int8"));
	//table.
     Struct struct = new Struct(FLAT_STRUCT_SCHEMA)
     .put("int8", (byte) 12)
     .put("int16", (short) 12)
     .put("int32", 12)
     .put("int64", (long) 12)
     .put("float32", 12.f)
     .put("float64", 12.)
     .put("boolean", true)
     .put("string", "foobar")
     .put("bytes", "foobar".getBytes());
     
     UpdateMutation updateM  = new UpdateMutation(table, Row.fromStruct(struct));
     InsertMutation insertM  = new InsertMutation(table,  Row.fromStruct(struct));
     DeleteMutation deleteM  = new DeleteMutation(table, Row.fromStruct(struct));

    private static String randGroupName(String topic){
    	return "test_group_" + topic + System.currentTimeMillis();
    }
    
	//@Test
   void testProducer() {	
		Handler<Mutation, Table, PassThroughMutationMapper> handler = new KafkaAvroHandler(new PassThroughMutationMapper(), KAFKA_CONFIG_FILE);  
		try{
			handler.processOp(updateM);
			handler.processOp(insertM);
			handler.processOp(deleteM);
		}catch (Exception e) {
				fail("Handler failed with error" + e);
		} 
	}
	
	//@Test
	void  testConsumer(){
		final String topic = KafkaUtil.genericTopic(table);
		final String zkConnect = "52.4.197.159:2181";
		final String groupId = randGroupName(topic);
          try {
		   consumer = new KafkaMutationAvroConsumer(topic, zkConnect, groupId ){
			   @Override
				protected void processInsertOp(InsertMutation op) {
					System.out.print(op);
					assertEquals("Insert Mutation should be the same",
							insertM, op);

				}
				@Override
				protected void processDeleteOp(DeleteMutation op) {
					System.out.print(op);
					assertEquals("Delete Mutation should be the same",
							deleteM, op);

				}

				@Override
				protected void processUpdateOp(UpdateMutation op) {
					System.out.print(op);
					assertEquals("Update Mutation should be the same",
							updateM, op);

				}

				@Override
				protected void processPkUpdateOp(Mutation op) {
					System.out.print(op);

				}   
			   
		   };
		   
		   consumer.start();
          }
          catch (kafka.common.InvalidConfigException e){
        	  System.out.print("Error Running consumer test, wrong config: " + e);
          }
          catch (Exception e) {
        	  System.out.print("Error Running consumer test: " + e);
        	  
          }
       
	}
	//TODO: add Test back.... there was some error with classPath()
	//@Test
	public void testAll(){
		testProducer();
		try{
		  Thread.sleep(4000);
		}catch (Exception e){
			  // Stupid Checked Exceptinos....
		}
		testConsumer();
	}
	public static void main(String [ ] args) {
		KafkaAvroHandlerTest test = new KafkaAvroHandlerTest();
		test.testAll();
	}

}
