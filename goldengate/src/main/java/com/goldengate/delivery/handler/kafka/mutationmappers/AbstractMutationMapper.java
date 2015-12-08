package com.goldengate.delivery.handler.kafka.mutationmappers;

import java.util.List;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.goldengate.atg.datasource.DsColumn;
import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.atg.datasource.meta.ColumnMetaData;
import com.goldengate.atg.datasource.meta.DsType;
import com.goldengate.atg.datasource.meta.TableMetaData;
import com.rogers.cdc.api.mutations.Column;
import com.rogers.cdc.api.mutations.DeleteMutation;
import com.rogers.cdc.api.mutations.InsertMutation;
import com.rogers.cdc.api.mutations.Mutation;
import com.rogers.cdc.api.mutations.MutationMapper;
import com.rogers.cdc.api.mutations.PkUpdateMutation;
import com.rogers.cdc.api.mutations.Row;
import com.rogers.cdc.api.mutations.UpdateMutation;
import com.rogers.cdc.api.schema.*;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.io.IOException;
import java.sql.Types;

public abstract class AbstractMutationMapper   extends MutationMapper<Op,TableMetaData > {
		final private static Logger logger = LoggerFactory.getLogger(AbstractMutationMapper.class);
	    private Row createRow(Op op, boolean onlyChanged ) throws IOException{
	  	    Row row = new Row();
	        TableMetaData tbl_meta = op.getTableMeta(); 
			  int i = 0;
			  for(DsColumn column : op) {
	               ColumnMetaData col_meta = tbl_meta.getColumnMetaData(i);; 
	              // logger.debug("column = " + op.getTableMeta().getColumnName(i) + ", changed = " + column.isChanged() + ", val= " + column.getAfterValue() );
	               //logger.debug("isKey = " + col_meta.isKeyCol() );
			    	 //Always include key Column
	                 if (!onlyChanged || column.isChanged() || col_meta.isKeyCol()){
			    		 String name = col_meta.getColumnName(); 
			    		 //String str_val = column.getAfterValue();
			    		 Column col = new Column(convertColumn(column,col_meta.getDataType().getJDBCType() ));                    
			    		 row.addColumn(name,col);
			    	 }
	             i++;
			    } 
	          logger.info("row: " + row.toString());
			   return row;
	    }
	    // TODO: Should to Proper type conversaion
	    protected abstract Object convertColumn(DsColumn col, int colType) throws IOException;
	    
	   @Override
	   public  Mutation  toMutation(Op op)  throws IOException {
	   	  Row row;
	   	  Table table = toTable(op.getTableMeta());
	   	  switch(op.getOpType()){
	           case DO_INSERT: 
	           	    row = createRow(op, false);
	      	        return new InsertMutation(table,  row);
	            case  DO_DELETE: 
	            	 row = createRow(op, false);
	       	        return new DeleteMutation(table,  row);
	            case DO_UPDATE: 
	            case DO_UPDATE_FIELDCOMP: 
	            case DO_UPDATE_AC: 
	           	   row = createRow(op, true);
	       	       return new UpdateMutation(table, row);
	            case DO_UPDATE_FIELDCOMP_PK:
	            	row = createRow(op, true);
	      	        return new PkUpdateMutation(table, row);
	             default:
	      	        //logger.error("The operation type " + op.getOpType() + " on  operation: table=[" + op.getTableName() + "]" + ", op ts=" + op.getTimestamp() + "is not supported");
	      	        throw new IllegalArgumentException("KafkaAvroHandler::getMagicByte Unknown operation type");                                                                            
	         }
	     }
	   
	
}
