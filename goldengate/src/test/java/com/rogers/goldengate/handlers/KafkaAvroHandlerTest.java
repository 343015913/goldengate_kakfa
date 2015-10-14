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

import com.rogers.goldengate.api.mutations.Column;
import com.rogers.goldengate.api.mutations.DeleteMutation;
import com.rogers.goldengate.api.mutations.InsertMutation;
import com.rogers.goldengate.api.mutations.Mutation;
import com.rogers.goldengate.api.mutations.Row;
import com.rogers.goldengate.api.mutations.UpdateMutation;
import com.rogers.goldengate.kafka.KafkaUtil;
import com.rogers.goldengate.kafka.consumers.KafkaMutationAvroConsumer;



public class KafkaAvroHandlerTest {
	private Properties config;

	InputStream inputStream;
	private final String KAFKA_CONFIG_FILE = "kafka.properties";
	
	private KafkaMutationAvroConsumer consumer;

	
	 String table = "testTable";
     String schema = "testSchema";
     Row.RowVal[] update_cols = {new Row.RowVal("name", new Column("Jon")),
             new Row.RowVal("age", new Column("28")),
             new Row.RowVal("balance", new Column("5.23"))
     };
     Row.RowVal[] insert_cols = {new Row.RowVal("name", new Column("Jon")),
          new Row.RowVal("age", new Column("28")),
          new Row.RowVal("balance", new Column("5.23"))
      };
     UpdateMutation updateM  = new UpdateMutation(table, schema, new Row(update_cols));
     InsertMutation insertM  = new InsertMutation(table, schema, new Row(insert_cols));
     DeleteMutation deleteM  = new DeleteMutation(table, schema);
	
	
	
  /*  private static Properties getConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", "52.4.197.159:2181");
        props.put("group.id", randGroupName());
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset","smallest");
        props.put("crypto.encryptor","avro");
        return props;
      
    }*/
    private static String randGroupName(String topic){
    	return "test_group_" + topic + System.currentTimeMillis();
    }
    
	//@Test
   void testProducer() {	
		Handler handler = new KafkaAvroHandler(KAFKA_CONFIG_FILE);  
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
		final String topic = KafkaUtil.genericTopic(schema, table);
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
		   
				
       // try{ 
        	

         //  VerifiableProperties vProps = new VerifiableProperties(getConsumerConfig());
           //DefaultDecoder keyDecoder = new DefaultDecoder(vProps);
  
           //Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);
      
           //final KafkaStream stream = consumerMap.get(topic).get(0);
           //ConsumerIterator<byte[], byte[]> it = stream.iterator();
           //while (it.hasNext()){
             //System.out.println("Consumer got: " + new String(it.next().message()));
             //assertEquals("Should get the original string in the consumer",
        		//   TEST_STRING, new String(it.next().message()));
           //}
       // }catch (Exception e) {
		//	fail("failed to  consume message stream, with error:" + e);
	    //} 
       
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
