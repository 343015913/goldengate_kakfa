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
import java.io.File;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
//import org.apache.avro.specific.SpecificDatumWriter;
//import org.apache.avro.specific.SpecificRecord;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.goldengate.atg.datasource.DsColumn;
import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.delivery.handler.kafka.HandlerProperties;
import com.goldengate.atg.datasource.meta.ColumnMetaData;
//import com.goldengate.atg.

/**
 * Abstract class which included most of the common functionality for
 * processing different operation types (INSERT, UPDATES, DELETES...).
 * 
 * Right now it's using a Generic Avro Schema to store all the columns as Strings (as a <name, val> pairs) 
 * 
 * @author Eugene Miretsky
 *
 */
public abstract class OperationHandler {
	 public static byte UnknownByte = 0x0;;
	 public static byte InsertByte = 0x1;
	 public static byte UpdateByte = 0x2;
	 public static byte DeleteByte = 0x3;
	 public static byte UpdatePKByte = 0x4; 
	
	 final public static Logger logger = LoggerFactory.getLogger(OperationHandler.class);
	 Schema schema;
	 Byte magicByte; 
	 

	 public OperationHandler(Byte _magicByte){
		 magicByte = _magicByte;
		 //schema = new Schema.Parser().parse(new File("GenericRecord.avsc"));
	 }
	 
	 protected void processOperation(Op op,HandlerProperties handlerProperties, String operationType, boolean useBeforeValues,  boolean includeFieldNames, boolean onlyChanged){
		logger.info("Kafka: processOperation");
		 String tableName = op.getTableName().getOriginalShortName().toLowerCase();
		 //ArrayList<ColumnMetaData> colms = op.getTableMeta().getColumnMetaData();
		 for (ColumnMetaData col:  op.getTableMeta().getColumnMetaData()){
			 logger.info("\t Column Name : " + col.getColumnName());
			 logger.info("\t\t Native Data Type : " + col.getNativeDataType());
			 logger.info("\t\t Data Type : " + col.getDataType());
			 logger.info("\t\t Binary Length  : " + col.getBinaryLength());

		 }
	//	StringBuilder content = prepareOutput(handlerProperties, useBeforeValues, operationType, op, includeFieldNames, onlyChanged);
		
		//ProducerRecordWrapper event = new ProducerRecordWrapper(tableName, content.toString().getBytes());
		//prepareEventHeader(op, event);
		//handlerProperties.events.add(event);
	}
	

	/** Adds a header into the given Record based on the Mutation's
	   *  database, table, and tableId.
	   *  @param record
	   *  @param mutation
	   */
	  protected void addHeader(GenericRecord record, Op op) {
		String tableName = op.getTableName().getOriginalShortName().toLowerCase();
	    String schemaName = op.getTableName().getOriginalSchemaName();
	    record.put("table", tableName);
	    record.put("schema", schemaName);
	  }
	  protected void addBodyImpl(GenericRecord record, Op op, boolean onlyChanged) {
		  Map<String,String> strings = new HashMap<String,String>();
		  int i = 0;
		     for(DsColumn column : op) {
		    	 if (!onlyChanged || column.isChanged()){
		    		 String name = op.getTableMeta().getColumnName(i); 
		    		 String val = column.getAfterValue();
		    		 strings.put(name,val);
		    	 }
		     } 
		  record.put("strings", strings);
	  }
	
	public void process(Op op, HandlerProperties handlerProperties){
		String tableName = op.getTableName().getOriginalShortName().toLowerCase();
		Object obj = avroRecord(op, handlerProperties); 
		//ProducerRecordWrapper event = new ProducerRecordWrapper(tableName, avroRecord(op, handlerProperties));
		//handlerProperties.events.add(event);
		
	}
	public  GenericRecord avroRecord(Op op, HandlerProperties handlerProperties){
		
		GenericRecord record = new GenericData.Record(schema);
		addHeader(record, op);
		addBody(record,op, handlerProperties );
		return record; 
	}
	protected abstract void addBody(GenericRecord record, Op op, HandlerProperties handlerProperties);
	 public  byte magicByte(){
		 return magicByte;
	 }
}
