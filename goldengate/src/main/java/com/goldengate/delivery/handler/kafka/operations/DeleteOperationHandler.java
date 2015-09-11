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
 * Delete operation handler, will be used when the operation type is "DELETE"
 * Increments the "totalDeletes" counter, which will be used while reporting
 * statistics.
 * 
 * @author Vedanth K R
 *
 */
public class DeleteOperationHandler extends OperationHandler {

	public DeleteOperationHandler(Byte magicByte) {
		super(magicByte);
	}

	@Override
	public void addBody(GenericRecord record, Op op, HandlerProperties handlerProperties) {
		// No body needed... I suppose could include the beforeValues.
		handlerProperties.totalDeletes++;

	}

}
