/*
*
* Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.
*
*/
package com.goldengate.delivery.handler.kafka.operations;

import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.delivery.handler.kafka.HandlerProperties;

/**
 * Insert operation handler, will be used when the operation type is "INSERT"
 * Increments the "totalInserts" counter, which will be used while reporting statistics.
 * 
 * @author Vedanth K R
 *
 */
public class InsertOperationHandler extends OperationHandler{

	@Override
	public void process(Op op, HandlerProperties handlerProperties) {
		logger.info("Kafka: process Delete");
		processOperation(op, handlerProperties, handlerProperties.insertOpKey,false, false, false);
		handlerProperties.totalInserts++;
	}

}
