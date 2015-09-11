/*
 *
 * Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.
 *
 */
package com.goldengate.delivery.handler.kafka.operations;

import org.apache.avro.generic.GenericRecord;

import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.delivery.handler.kafka.HandlerProperties;

/**
 * Primary Key Update operation handler, will be used when the operation type is
 * "UPDATE_FIELDCOMP_PK" (Primary Key Updates) Increments the "totalUpdates"
 * counter, which will be used while reporting statistics.
 * 
 * @author Vedanth K R
 *
 */
public class PkUpdateOperationHandler extends OperationHandler {

	public PkUpdateOperationHandler(Byte magicByte) {
		super(magicByte);
	}

	@Override
	public void addBody(GenericRecord record, Op op, HandlerProperties handlerProperties) {
		assert (true);// Not Supported

	}

}
