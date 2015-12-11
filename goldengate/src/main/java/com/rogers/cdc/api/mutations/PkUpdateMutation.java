package com.rogers.cdc.api.mutations;

import com.rogers.cdc.api.schema.*;

public class PkUpdateMutation extends UpdateMutation {
	public PkUpdateMutation(Table table){
		this(table, null);

    }
	 public PkUpdateMutation(Table table, Row  _row){
	    	super(table, _row);
	    	
	    	magicByte = UpdatePKByte;
	    	if (row.size() != 1){
	    		throw new RuntimeException("PkUpdateMutation can have only 1 column");
	    	}
	    }
	  @Override
	  public  MutationType getType(){
	    	return MutationType.PKUPDATE;
	    	
	    }
	    @Override 
	    public  String toString(){
	       final StringBuilder sb = new StringBuilder();
	       sb.append("PkUpdateMutation");
	        sb.append("{schema=").append(this.getSchemaName());
	        sb.append(", table=").append(this.getTableName());
	        sb.append(", row=[");
	        sb.append("\n").append(row);
	        sb.append("]}");
	       return sb.toString();
	    }
}
