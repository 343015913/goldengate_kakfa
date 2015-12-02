package com.rogers.cdc.handlers;

import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rogers.cdc.api.mutations.MutationMapper;
import com.rogers.kafka.Producer;

//TODO: Make me Generic with plugable serialzier
public abstract class KafkaHandler<Op, OpMapper extends MutationMapper<Op>> extends Handler<Op, OpMapper> {
	  final private static Logger logger = LoggerFactory
			.getLogger(KafkaHandler.class);
	  
	  private Producer  producer;
	  protected KafkaHandler(OpMapper _opMapper){
			super(_opMapper);
		}
	  KafkaHandler(OpMapper _opMapper, String configFile) {
		  super(_opMapper);
		  producer = new Producer(configFile);
	  }
	  protected void send(String topic, byte[] key, byte[] msg) {
		  try{
		    producer.send(topic,key, msg);//TODO: Add suport for keys
		  }catch(ExecutionException e){
		    	throw new RuntimeException("Failed to send Kafka Message: " + e );
		  }catch(InterruptedException e){
		    	throw new RuntimeException("Failed to send Kafka Message: " + e );
		  }catch(Exception e){
		    	throw new RuntimeException("Failed to send Kafka Message: " + e );
		  }
	  }
	   
		@Override
		public  void flush(){
			// Nothing to do here...
			// We're using the new Java Kafka Producer (8.2) it is async and does it's own buffering. Configure the producer to fine-tune buffering behaviour  	
		}
		
		
		
}
