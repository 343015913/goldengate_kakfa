/*
*
* Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.
*
*/
package com.goldengate.delivery.handler.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




import com.goldengate.atg.datasource.AbstractHandler;
import com.goldengate.atg.datasource.DsConfiguration;
import com.goldengate.atg.datasource.DsEvent;
import com.goldengate.atg.datasource.DsOperation;
import com.goldengate.atg.datasource.DsTransaction;
import com.goldengate.atg.datasource.GGDataSource.Status;
import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.atg.datasource.meta.DsMetaData;
import com.goldengate.delivery.handler.kafka.operations.OperationHandler;
import com.goldengate.delivery.handler.kafka.util.OperationTypes;
import com.goldengate.delivery.handler.kafka.KafkaProducerWrapper;
import com.goldengate.delivery.handler.kafka.ProducerRecordWrapper;

/**
 * KafkaHandler is an extension of GoldenGate Java Adapters - "EventHandlers".
 * 
 * This handler connects to a running Flume instance via 
 * Avro/Thrift RPC. It operates on the column values received from the operations,
 * creates Flume events out of the column values and publishes the events to
 * Flume when the transaction commit occurs.
 * 
 * In "tx" mode, the events will be buffered till the transaction commit is received 
 * and all the events will be published in batch, once the transaction is committed.
 * In "op" mode, the events will be published on every operation/record basis.
 * 
 * Considering a table "TCUST" with columns "CUST_ID", "CUST_NAME", "ADDRESS" with a 
 * record being inserted into it say "10001","Flume Admin","Los Angles".
 * The final data published into Flume would be similar to the following.
 * (Assuming "," as the configured delimiter)
 * ### OperationType,Col-1, Col-2, Col-3, Operation Timestamp  ###
 * I,10001,Flume Admin,Los Angles,2014-12-18 08:28:02.000000
 * 
 * The Operation Type and Operation Timestamp are configurable. 
 * By default both Operation Type and Operation Timestamp will be part of the delimited separated values.
 * 
 * @author Vedanth KR
 * 
 * */
public class KafkaHandler  extends AbstractHandler{

	 final private static Logger logger = LoggerFactory.getLogger(KafkaHandler.class);
	
	
	/** Producer for Kakfa */
	private KafkaProducerWrapper producer; 
	
	/** 
	 * Eg: 
	 * 1. ; or , or " ...
	 * 2. Non printable ASCII characters like \u0001, \u0002
	 */
	private String delimiter;
	
	/**
	 * Indicates the actual key that should be used in the delimited separated values
	 * to identify the operation type. Default values are:
	 * I -> Insert
	 * U -> Field(s) Update
	 * D -> Delete
	 * P -> PrimaryKey Update
	 * 
	 * Above keys can be overridden in the properties file. 
	 * */
	private String deleteOpKey = "D";
	private String insertOpKey = "I";
	private String updateOpKey = "U";
	private String pKUpdateOpKey = "P";
	
	/**
	 *  Indicates if the operation type should be included as part of output in the delimited separated values
	 *	true - Operation type will be included in the output
	 *	false - Operation type will not be included in the output
	 **/
	private Boolean includeOpType=true;
	
	/**
	 *  Indicates if the operation timestamp should be included as part of output in the delimited separated values
	 *	true - Operation timestamp will be included in the output
	 *	false - Operation timestamp will not be included in the output
	 **/
	private Boolean includeOpTimestamp=true;
	

	
	/** Collection to store flume events until the "transaction commit" event is received in "tx" mode, upon which the list will be cleared*/
	private List<ProducerRecordWrapper> events = new ArrayList<ProducerRecordWrapper>();	
	private HandlerProperties handlerProperties;

	@Override
	public void init(DsConfiguration arg0, DsMetaData arg1) {
		logger.info("Initializing Kafka Handler: mode=" + getMode());
		super.init(arg0, arg1);
	/*	
		if(Rpc.avro.toString().equals(getRpcType())) {
			// Initialization of Avro RPC client. 
			this.rpcClient = RpcClientFactory.getDefaultInstance(getHost(), getPort());
		}
		//TODO: Yet to be Tested 
		else if(Rpc.thrift.toString().equals(getRpcType())) {
			// Initialization of Thrift RPC client. 
			this.rpcClient = RpcClientFactory.getThriftInstance(getHost(), getPort());
		}
		*/
		try {
		   producer = new KafkaProducerWrapper();
	    } catch (IOException e) {
		   System.out.println("Exception: " + e);
	    } 
		/** Setting initial properties in HandlerProperties object, which will be used for communicating the 
		 * information from Flume AbstractHandler to Operation Handlers 
		 * */
		initializeHandlerProperties();
	}

	@Override
	public Status operationAdded(DsEvent e, DsTransaction tx, DsOperation dsOperation) {
		 logger.info("Operation added event. Operation type = " + dsOperation.getOperationType());
		 
		 Status status = Status.OK;
		super.operationAdded(e, tx, dsOperation);
		Op op = new Op(dsOperation, getMetaData().getTableMetaData(dsOperation.getTableName()), getConfig());
		
		/** Get the class defined for the incoming operation type from OperationTypes enum 
		 * and instantiate the operation handler class.
		 * Each Operation Handler class creates the flume event.
		 * */
		OperationTypes operationTypes = OperationTypes.valueOf(dsOperation.getOperationType().toString());
		OperationHandler operationHandler = operationTypes.getOperationHandler();
		if(operationHandler != null){
			try {
				operationHandler.process(op, handlerProperties);
				/** Increment the total number of operations */
				handlerProperties.totalOperations++;
			} catch (Exception e1) {
				status = Status.ABEND;
				logger.error("Unable to process operation.", e1);
			}
		}else{
			status = Status.ABEND;
			logger.error("Unable to instantiate operation handler.");
		}
		
		return status;
	}
	
	
	@Override
	public Status transactionCommit(DsEvent e, DsTransaction tx) {
		 logger.info("Transaction commit event ");
		super.transactionCommit(e, tx);
		Status status = Status.OK;
		/** Publish the events to flume using the rpc client once the transaction commit event is received.
		 * 
		 * In "op" mode,
		 *     Event will be published to flume on every "operationAdded" event, as transaction commit would be 
		 *     invoked for every operation.
		 *     
		 * In "tx" mode,
		 *     Events will be buffered on every "opeartionAdded" event and published to flume once the transaction 
		 *     commit event is received. 
		 * 	
		 * */
		status = publishEvents();
		handlerProperties.totalTxns++;
		
		return  status;
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
	        sb.append("Status report: mode=").append(getMode());
	        sb.append(", transactions=").append(handlerProperties.totalTxns);
	        sb.append(", operations=").append(handlerProperties.totalOperations);
	        sb.append(", inserts=").append(handlerProperties.totalInserts);
	        sb.append(", updates=").append(handlerProperties.totalUpdates);
	        sb.append(", deletes=").append(handlerProperties.totalDeletes);
	        
	        logger.info("Final Status " + sb.toString());
	        return sb.toString();
	}
	
	@Override
	public void destroy() {
		logger.info("Destroy event");
		/**
		 * Close the connection to flume. 
		 * */
		//this.rpcClient.close();
		super.destroy();
	}
	
	private Status publishEvents() {
		logger.info("Publishing events to Kafka. Events size = " + this.events);
		Status status = Status.OK;
		/**
		 * Publishing the events to Kafka.
		 * */
		if(!this.events.isEmpty()) {			
			try {
				for (ProducerRecordWrapper rec: this.handlerProperties.events){
				   this.producer.send(rec);
				}
			} catch (Exception e1) {
				status = Status.ABEND;
				logger.error("Unable to deliver events. Events size : " + events.size(), e1);
			}
			
			this.handlerProperties.events.clear();
		}
		else {
			logger.warn("No events available to publish.");
		}
		return status;	
	}
	
	private void initializeHandlerProperties() {
		this.handlerProperties = new HandlerProperties();
		this.handlerProperties.delimiter = this.delimiter;
		this.handlerProperties.events = this.events;
		this.handlerProperties.deleteOpKey = this.deleteOpKey;
		this.handlerProperties.insertOpKey = this.insertOpKey;
		this.handlerProperties.updateOpKey = this.updateOpKey;
		this.handlerProperties.pKUpdateKey = this.pKUpdateOpKey;
		this.handlerProperties.includeOpTimestamp = this.includeOpTimestamp;
		this.handlerProperties.includeOpType = this.includeOpType;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}
	
	public String getDeleteOpKey() {
		return deleteOpKey;
	}

	public void setDeleteOpKey(String deleteOpKey) {
		this.deleteOpKey = deleteOpKey;
	}

	public String getInsertOpKey() {
		return insertOpKey;
	}

	public void setInsertOpKey(String insertOpKey) {
		this.insertOpKey = insertOpKey;
	}

	public String getUpdateOpKey() {
		return updateOpKey;
	}

	public void setUpdateOpKey(String updateOpKey) {
		this.updateOpKey = updateOpKey;
	}

	public String getpKUpdateOpKey() {
		return pKUpdateOpKey;
	}

	public void setpKUpdateOpKey(String pKUpdateOpKey) {
		this.pKUpdateOpKey = pKUpdateOpKey;
	}

	public List<ProducerRecordWrapper> getEvents() {
		return events;
	}

	public void setEvents(List<ProducerRecordWrapper> events) {
		this.events = events;
	}

	public HandlerProperties getHandlerProperties() {
		return handlerProperties;
	}

	public void setHandlerProperties(HandlerProperties handlerProperties) {
		this.handlerProperties = handlerProperties;
	}

	public Boolean getIncludeOpType() {
		return includeOpType;
	}

	public void setIncludeOpType(Boolean includeOpType) {
		this.includeOpType = includeOpType;
	}

	public Boolean getIncludeOpTimestamp() {
		return includeOpTimestamp;
	}

	public void setIncludeOpTimestamp(Boolean includeOpTimestamp) {
		this.includeOpTimestamp = includeOpTimestamp;
	}
	
}
