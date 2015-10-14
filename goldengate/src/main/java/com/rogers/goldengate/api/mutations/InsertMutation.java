package com.rogers.goldengate.api.mutations;

import java.util.List;

public class InsertMutation extends RowMutation {

    public InsertMutation(String table, String schema){
    	this(table, schema, null);
    }
    public InsertMutation(String table, String shema, Row  _row){
    	super(table, shema, _row);
        magicByte = InsertByte; 
    }
	  @Override
	  public MutationType getType(){
	    	return MutationType.INSERT;
	    	
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
