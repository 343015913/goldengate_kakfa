/*
*
* Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.
*
*/
package com.goldengate.delivery.handler.kafka.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.goldengate.delivery.handler.kafka.operations.DeleteOperationHandler;
import com.goldengate.delivery.handler.kafka.operations.InsertOperationHandler;
import com.goldengate.delivery.handler.kafka.operations.OperationHandler;
import com.goldengate.delivery.handler.kafka.operations.PkUpdateOperationHandler;
import com.goldengate.delivery.handler.kafka.operations.UpdateOperationHandler;

/**
 * Enum to maintain the operation type against the handlers.
 * 
 * @author Vedanth K R
 *
 */
public enum OperationTypes {

	INSERT(InsertOperationHandler.class),
	UPDATE_FIELDCOMP(UpdateOperationHandler.class),
	UPDATE_FIELDCOMP_PK(PkUpdateOperationHandler.class),
	DELETE(DeleteOperationHandler.class);
	
	final private Logger logger = LoggerFactory.getLogger(OperationTypes.class);
	
	private OperationHandler operationHandler;
	private OperationTypes(Class<?> clazz)
	{
		try {
			operationHandler = (OperationHandler) clazz.newInstance();
		} catch (Exception e) {
			logger.error("Unable to instantiate operation handler. "+ clazz , e);
		}
	}
	
	public OperationHandler getOperationHandler()
	{
		return operationHandler;
	}
}
