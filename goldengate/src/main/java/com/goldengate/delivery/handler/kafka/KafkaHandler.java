/*
 *
 * Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.
 *
 */
package com.goldengate.delivery.handler.kafka;


import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.goldengate.atg.datasource.AbstractHandler;
import com.goldengate.atg.datasource.DsColumn;
import com.goldengate.atg.datasource.DsConfiguration;
import com.goldengate.atg.datasource.DsEvent;
import com.goldengate.atg.datasource.DsOperation;
import com.goldengate.atg.datasource.DsTransaction;
import com.goldengate.atg.datasource.GGDataSource.Status;
import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.atg.datasource.meta.DsMetaData;
import com.goldengate.atg.datasource.adapt.Tx;
import com.goldengate.atg.datasource.meta.ColumnMetaData;
import com.goldengate.atg.datasource.meta.TableMetaData;
import com.goldengate.atg.datasource.meta.TableName;
import com.goldengate.delivery.handler.kafka.ProducerRecordWrapper;
import com.rogers.goldengate.handlers.Handler;
import com.rogers.goldengate.handlers.KafkaAvroHandler;
import com.rogers.goldengate.api.mutations.Mutation;

//TODO: Fix the desc
/**
 * KafkaHandler is an extension of GoldenGate Java Adapters - "EventHandlers".
 * It operates on the column values received from the operations, creates Kafka
 * messages out of the column values and publishes the events to Kafka when the
 * transaction commit occurs.
 * 
 * In "tx" mode, the events will be buffered till the transaction commit is
 * received and all the events will be published in batch, once the transaction
 * is committed. In "op" mode, the events will be published on every
 * operation/record basis.
 * 
 * The Kafka Java Client (as of Kafka 0.8.2) is asynchronous and support message
 * batching/buffering. As such, the "tx" and "op" modes should have comparable
 * performance.
 * 
 * 
 * Considering a table "TCUST" with columns "CUST_ID", "CUST_NAME", "ADDRESS"
 * with a record being inserted into it say "10001","Kafka Admin","Los Angles".
 * The final data published into Flume would be similar to the following.
 * (Assuming "," as the configured delimiter) ### OperationType,Col-1, Col-2,
 * Col-3, Operation Timestamp ### I,10001,Kafka Admin,Los Angles,2014-12-18
 * 08:28:02.000000
 * 
 * The Operation Type and Operation Timestamp are configurable. By default both
 * Operation Type and Operation Timestamp will be part of the delimited
 * separated values.
 * 
 * @author Eugene Miretsky
 * 
 * */

public class KafkaHandler extends AbstractHandler {
	final private static Logger logger = LoggerFactory
			.getLogger(KafkaHandler.class);

	// TODO: Move it to a test class


	/**
	 * Config file for Kafka Producer
	 * http://kafka.apache.org/082/javadoc/org/apache
	 * /kafka/clients/producer/KafkaProducer.html
	 */
	//TODO: Should be generic...for Kafka/HDFS/etc
	private String kafkaConfigFile;
	private final String KAFKA_CONFIG_FILE = "kafka.properties";

	/**
	 * Indicates if the operation timestamp should be included as part of output
	 * in the delimited separated values true - Operation timestamp will be
	 * included in the output false - Operation timestamp will not be included
	 * in the output
	 **/
	private Boolean includeOpTimestamp = true;

	/**
	 * Collection to store kafka messages until the "transaction commit" event
	 * is received in "tx" mode, upon which the list will be cleared
	 */
	//private List<ProducerRecordWrapper> events = new ArrayList<ProducerRecordWrapper>();
	private HandlerProperties handlerProperties;

	
	private Handler handler; 
	@Override
	public void init(DsConfiguration arg0, DsMetaData arg1) {
		// TODO: Do something with the config file
		kafkaConfigFile = KAFKA_CONFIG_FILE; // set default value
		handler = new KafkaAvroHandler(kafkaConfigFile);
		super.init(arg0, arg1);
		// TODO: anything we need to do here?
		logger.info("Done Initializing Kafka Handler");
	}

	@Override
	public Status operationAdded(DsEvent e, DsTransaction transaction, DsOperation operation) {
		
		logger.debug("Operation added event. Operation type = "
				+ operation.getOperationType());
        Status status = Status.OK;                                                                    
        super.operationAdded(e, transaction, operation);                                              
    
        if(isOperationMode()) {
            // Tx/Op/Col adapters wrap metadata & values behind a single, simple                      
            // interface if using the DataSourceListener API (via AbstractHandler).                   
            final Tx tx = new Tx(transaction, getMetaData(), getConfig());                            
            final TableMetaData tMeta = getMetaData().getTableMetaData(operation.getTableName());     
            final Op op = new Op(operation, tMeta, getConfig());                                      
            status = processOp(tx, op); // process data...
        }

        return status;
	
	}

	@Override
	public Status transactionCommit(DsEvent e, DsTransaction transaction) {
		logger.debug("Transaction commit event ");                           
        super.transactionCommit(e, transaction);
        Status status = Status.OK;  
        
        // Increment the number of transactions
		handlerProperties.totalTxns++;
                                                                     
        Tx tx = new Tx(transaction, getMetaData(), getConfig());                                      
        
        // In 'operation mode', all the operations would have been processed when                     
        // 'operationAdded' is called. In 'transaction mode', they are processed                      
        // when the commit event is received.
        if(!isOperationMode()) {                                                                      
            for(Op op: tx) {
                status = processOp(tx, op); // process data...                                        
                if (status != Status.OK){                                                             
                    //Break out of this loop                                                          
                    break;                                                                            
                }
            }
        }
        
          logger.debug("  Received transaction commit event, transaction count="
                    + handlerProperties.totalTxns
                    + ", pos=" + tx.getTranID()
                    + " (total_ops= "+ tx.getTotalOps()
                    + ", buffered="+ tx.getSize() + ")"
                    + ", ts=" + tx.getTimestamp());

        return status;
		
	}

	@Override
	public Status metaDataChanged(DsEvent e, DsMetaData meta) {
		logger.info("Metadata change event");
		return super.metaDataChanged(e, meta);
	}

	@Override
	public Status transactionBegin(DsEvent e, DsTransaction tx) {
		logger.info("Transaction begin event");
		return super.transactionBegin(e, tx);
	}

	@Override
	public String reportStatus() {
		logger.info("Reporting Status ");
		
		StringBuilder sb = new StringBuilder();
		//TODO:
		/*
		sb.append("Status report: mode=").append(getMode());
		sb.append(", transactions=").append(handlerProperties.totalTxns);
		sb.append(", operations=").append(handlerProperties.totalOperations);
		sb.append(", inserts=").append(handlerProperties.totalInserts);
		sb.append(", updates=").append(handlerProperties.totalUpdates);
		sb.append(", deletes=").append(handlerProperties.totalDeletes);

		logger.info("Final Status " + sb.toString());
		*/
		return sb.toString();
	}

	@Override
	public void destroy() {
		logger.info("Destroy event");
		super.destroy();
	}
/*
	private Status publishEvents() {
		logger.info("Publishing events to Kafka. Events size = " + this.events);
		Status status = Status.OK;
		
		 // Publishing the events to Kafka.
		 
		if (!this.events.isEmpty()) {
			try {
				for (ProducerRecordWrapper rec : this.handlerProperties.events) {
					this.producer.send(rec);
				}
			} catch (Exception e1) {
				status = Status.ABEND;
				logger.error("Unable to deliver events. Events size : "
						+ events.size(), e1);
			}

			this.handlerProperties.events.clear();
		} else {
			logger.warn("No events available to publish.");
		}
		return status;
	}
	*/
	/**
     * Private method to distribute the current operation to a handler and write the                  
     * operation data to an HDFS file.
     * @param currentTx The current transaction.                                                      
     * @param op The current operation.
     * @return Status.OK on success, else Status.ABEND                                                
     */
    private Status processOp(Tx currentTx, Op op) {  
    	Status status = Status.OK;  
        if(logger.isDebugEnabled()){ 
            logger.debug("Process operation: table=[" + op.getTableName() + "]"                       
                + ", op pos=" + op.getPosition()
                + ", tx pos=" + currentTx.getTranID()                                                 
                + ", op ts=" + op.getTimestamp());  
              TableMetaData tMeta = getMetaData().getTableMetaData(op.getTableName()); 
              int index = 0;
              for(DsColumn column : op) { 
            		ColumnMetaData cMeta = tMeta.getColumnMetaData(index++); 
            		logger.debug(" cMeta.getOriginalColumnName() " + 
            		cMeta.getOriginalColumnName() + ". column.getAfterValue() -> "+ 
            		column.getAfterValue()+", column.getBeforeValue() -> " + 
            		column.getBeforeValue()); 
              } 
        }                                                                                             
        try { 
            TableName  tname = op.getTableName();
            TableMetaData tMeta = getMetaData().getTableMetaData(tname);
            Mutation mutation = Mutation.fromOp(op);
            handler.processOp(mutation);
        }catch(RuntimeException e){  
        	     status = Status.ABEND;
                 logger.error("Failed to Process operation: table=[" + op.getTableName() + "]"             
                   + ", op pos=" + op.getPosition()                                                      
                   + ", tx pos=" + currentTx.getTranID()                                                 
                   + ", op ts=" + op.getTimestamp() 
                   + " with error: " + e);	
        }
                                      
                                                                                                      
        return status;
    }
/*
	private void initializeHandlerProperties() {
		this.handlerProperties = new HandlerProperties();
		this.handlerProperties.events = this.events;
		this.handlerProperties.includeOpTimestamp = this.includeOpTimestamp;
	}*/

	public String getKafkaConfigFile() {
		return kafkaConfigFile;
	}

	public void setKafkaConfigFile(String delimiter) {
		this.kafkaConfigFile = delimiter;
	}

	/*public List<ProducerRecordWrapper> getEvents() {
		return events;
	}

	public void setEvents(List<ProducerRecordWrapper> events) {
		this.events = events;
	}*/

	public HandlerProperties getHandlerProperties() {
		return handlerProperties;
	}

	public void setHandlerProperties(HandlerProperties handlerProperties) {
		this.handlerProperties = handlerProperties;
	}

	public Boolean getIncludeOpTimestamp() {
		return includeOpTimestamp;
	}

	public void setIncludeOpTimestamp(Boolean includeOpTimestamp) {
		this.includeOpTimestamp = includeOpTimestamp;
	}

}
