package com.rogers.goldengate.handlers;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ExecutionException;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
    
import com.rogers.kafka.Producer;

//TODO: Make me Generic with plugable serialzier
public abstract class KafkaHandler extends Handler {
	  final private static Logger logger = LoggerFactory
			.getLogger(KafkaHandler.class);
	  
	  private Producer  producer;
	  
	  KafkaHandler(String configFile) {
		  producer = new Producer(configFile);
	  }
	  protected void send(String topic, byte[] msg) {
		  try{
		    producer.send(topic, msg);
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
