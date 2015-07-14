/*
*
* Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.
*
*/
package com.goldengate.delivery.handler.kafka.operations;

import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.delivery.handler.kafka.HandlerProperties;

/**
 * Primary Key Update operation handler, will be used when the operation type is "UPDATE_FIELDCOMP_PK" (Primary Key Updates)
 * Increments the "totalUpdates" counter, which will be used while reporting statistics.
 * 
 * @author Vedanth K R
 *
 */
public class PkUpdateOperationHandler extends OperationHandler{

	@Override
	public void process(Op op, HandlerProperties handlerProperties) {
		processOperation(op, handlerProperties, handlerProperties.pKUpdateKey,false);
		handlerProperties.totalUpdates++;
	}
	
}
