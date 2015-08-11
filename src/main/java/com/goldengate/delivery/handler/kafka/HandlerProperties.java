/*
*
* Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.
*
*/
package com.goldengate.delivery.handler.kafka;

import java.util.List;

import org.apache.kafka.clients.producer.ProducerRecord;

public class HandlerProperties {
    
	public List<ProducerRecordWrapper> events; 
	public String delimiter;
	public String insertOpKey;
	public String updateOpKey;
	public String deleteOpKey;
	public String pKUpdateKey;
	public Boolean includeOpType;
	public Boolean includeOpTimestamp;
	
	public Long totalInserts = 0L;
	public Long totalUpdates = 0L;
	public Long totalDeletes =  0L;
	public Long totalTxns = 0L;
	public Long totalOperations = 0L;
	
}
