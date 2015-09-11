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
 * Update operation handler, will be used when the operation type is
 * "UPDATE_FIELDCOMP" (Field Updates) Increments the "totalUpdates" counter,
 * which will be used while reporting statistics.
 * 
 * @author Vedanth K R
 *
 */
public class UpdateOperationHandler extends OperationHandler {

	public UpdateOperationHandler(Byte magicByte) {
		super(magicByte);
	}

	@Override
	public void addBody(GenericRecord record, Op op, HandlerProperties handlerProperties) {
		logger.info("Kafka: process Update");
		addBodyImpl(record, op , true);
		handlerProperties.totalUpdates++;
	}

}
