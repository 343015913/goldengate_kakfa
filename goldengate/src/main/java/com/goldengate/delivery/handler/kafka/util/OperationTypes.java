/*
 *
 * Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.
 *
 */
package com.goldengate.delivery.handler.kafka.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;

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

	INSERT(InsertOperationHandler.class, OperationHandler.InsertByte),
	UPDATE_FIELDCOMP(UpdateOperationHandler.class, OperationHandler.UpdateByte), 
	UPDATE_FIELDCOMP_PK(PkUpdateOperationHandler.class, OperationHandler.UpdatePKByte), 
	DELETE(DeleteOperationHandler.class, OperationHandler.DeleteByte);

	static byte UnknownByte = 0x0;;
	static byte InsertByte = 0x1;
	static byte UpdateByte = 0x2;
	static byte DeleteByte = 0x3;

	final private Logger logger = LoggerFactory.getLogger(OperationTypes.class);

	private OperationHandler operationHandler;

	private OperationTypes(Class<?> clazz, Byte magicByte) {
		try {
			Constructor[] ctors = clazz.getDeclaredConstructors();
			Constructor ctor = null;
			for (int i = 0; i < ctors.length; i++) {
			    ctor = ctors[i];
			    if (ctor.getGenericParameterTypes().length == 1)
				break;
			}
			ctor.setAccessible(true);
			operationHandler = (OperationHandler) ctor.newInstance(magicByte);
		} catch (Exception e) {
			logger.error("Unable to instantiate operation handler. " + clazz, e);
		}
	}

	public OperationHandler getOperationHandler() {
		return operationHandler;
	}
}
