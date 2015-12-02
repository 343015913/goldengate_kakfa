package com.rogers.goldengate.handlers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
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

	
	// String table = "testTable";
     //String schema = "testSchema";
	//TODO: Add Schema
     Table table = new Table("testSchema", "testTable", null, null);
     Row.RowVal[] update_cols = {new Row.RowVal("name", new Column("Jon")),
             new Row.RowVal("age", new Column("28")),
             new Row.RowVal("balance", new Column("5.23"))
     };
     Row.RowVal[] insert_cols = {new Row.RowVal("name", new Column("Jon")),
          new Row.RowVal("age", new Column("28")),
          new Row.RowVal("balance", new Column("5.23"))
      };
     UpdateMutation updateM  = new UpdateMutation(table, new Row(update_cols));
     InsertMutation insertM  = new InsertMutation(table,  new Row(insert_cols));
     DeleteMutation deleteM  = new DeleteMutation(table);

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
