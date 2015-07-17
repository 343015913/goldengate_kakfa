/*
*
* Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.
*
*/
package com.goldengate.delivery.handler.kafka.operations;

import com.goldengate.delivery.handler.kafka.KafkaHandler;
import com.goldengate.delivery.handler.kafka.ProducerRecordWrapper;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;





import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.goldengate.atg.datasource.DsColumn;
import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.delivery.handler.kafka.HandlerProperties;

/**
 * Abstract class which included most of the common functionality for
 * processing different operation types (INSERT, UPDATES, DELETES...).
 * Functionality includes:
 * 1.	TODO: .
 * 2.	Setting Schema Name and Table Name in the Flume Event Headers.
 * 
 * @author Vedanth K R
 *
 */
public abstract class OperationHandler {
	
	 final public static Logger logger = LoggerFactory.getLogger(OperationHandler.class);
	protected void processOperation(Op op,HandlerProperties handlerProperties, String operationType, boolean useBeforeValues){
		logger.info("Kafka: processOperation");
		StringBuilder content = prepareOutput(handlerProperties, useBeforeValues, operationType, op);
		
		ProducerRecordWrapper event = new ProducerRecordWrapper("test_topic", content.toString().getBytes());
		//prepareEventHeader(op, event);
		handlerProperties.events.add(event);
	}
	
	/**
     * Appends the TABLE_NAME and SCHEMA_NAME in the Flume Event Header.
     * 
     * @param op The current operation.
     * @param event Flume Event 
     * @return void
     */
	/*
	private void prepareEventHeader(Op op, Event event){
		
		String tableName = op.getTableName().getOriginalShortName().toLowerCase();
		String schemaName = op.getTableName().getOriginalSchemaName();
		
		Map<String, String> eventHeader = new HashMap<String, String>();
		eventHeader.put("TABLE_NAME", tableName);
		
		if(schemaName != null  && !schemaName.equals("")){
			eventHeader.put("SCHEMA_NAME", schemaName.toLowerCase());	
		}
		
		event.setHeaders(eventHeader);
	}*/
	
	/**
     * Prepares output in delimited separated value(DSV) format.
     * 
     * @param handlerProperties
     * @param useBeforeValues 
     * @param operationType
     * @param op The current operation. 
     * @return StringBuilder
     */
	private StringBuilder prepareOutput(HandlerProperties handlerProperties,  boolean useBeforeValues, String operationType, Op op){
		StringBuilder builder = new StringBuilder();
		
		if(handlerProperties.includeOpType){
			builder.append(operationType).append(handlerProperties.delimiter);	
		}
		
		if(useBeforeValues) {
			appendBeforeValues(op, handlerProperties, builder);
		}
		else {
			appendAfterValues(op, handlerProperties, builder);
		}
		
		if(handlerProperties.includeOpTimestamp){
			builder.append(handlerProperties.delimiter).append(op.getTimestamp());	
		}
		
		return builder;
	}
	
	private void appendBeforeValues(Op op,HandlerProperties handlerProperties,StringBuilder builder) {
		int i = 0;
		for(DsColumn column : op) {
			builder.append(column.getBeforeValue());
			i++;
			if(op.getNumColumns() != i){
				builder.append(handlerProperties.delimiter);
			}
		}
	}
	
	private void appendAfterValues(Op op,HandlerProperties handlerProperties,StringBuilder builder) {
		int i = 0;
		for(DsColumn column : op) {
			builder.append(column.getAfterValue());
			i++;
			if(op.getNumColumns() != i){
				builder.append(handlerProperties.delimiter);
			}
		}
	}
	
	public abstract void process(Op op, HandlerProperties handlerProperties);
}
