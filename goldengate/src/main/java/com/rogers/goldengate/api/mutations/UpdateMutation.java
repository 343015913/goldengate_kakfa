package com.rogers.goldengate.api.mutations;

import java.util.Arrays;
import java.util.List;



public class UpdateMutation extends RowMutation {

    
    public UpdateMutation(String table,String schema,  Row  _row ){
    	super(table, schema, _row);
        magicByte = UpdateByte; 
    }
    public UpdateMutation(String table,String schema ){
    	this(table, schema, null);
    	
    }
    @Override
    public MutationType getType(){
    	return MutationType.UPDATE;	
    }
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("UpdateMutation");
        sb.append("{schema=").append(schemaName);
        sb.append(", table=").append(tableName);
        sb.append(", row=[");
        sb.append("\n").append(row);
        sb.append("]}");
        return sb.toString();
    }
   
    
}
