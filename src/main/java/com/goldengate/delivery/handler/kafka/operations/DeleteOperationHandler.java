/*
*
* Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.
*
*/
package com.goldengate.delivery.handler.kafka.operations;

import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.delivery.handler.kafka.HandlerProperties;

/**
 * Delete operation handler, will be used when the operation type is "DELETE"
 * Increments the "totalDeletes" counter, which will be used while reporting statistics.
 * 
 * @author Vedanth K R
 *
 */
public class DeleteOperationHandler extends OperationHandler{

	@Override
	public void process(Op op, HandlerProperties handlerProperties) {
		
		processOperation(op, handlerProperties, handlerProperties.deleteOpKey,true, false);
		handlerProperties.totalDeletes++;
		
	}

}
