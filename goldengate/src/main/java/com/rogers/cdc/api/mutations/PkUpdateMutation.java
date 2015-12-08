package com.rogers.cdc.api.mutations;

import com.rogers.cdc.api.schema.*;

public class PkUpdateMutation extends RowMutation {
	public PkUpdateMutation(Table table){
		this(table, null);

    }
	 public PkUpdateMutation(Table table, Row  _row){
	    	super(table, _row);
	    	magicByte = UpdatePKByte;
	    }
	  @Override
	  public  MutationType getType(){
	    	return MutationType.PKUPDATE;
	    	
	    }
	    @Override 
	    public  String toString(){
	       final StringBuilder sb = new StringBuilder();
	       sb.append("UpdateMutation");
	        sb.append("{schema=").append(this.getSchemaName());
	        sb.append(", table=").append(this.getTableName());
	        sb.append("}");
	       return sb.toString();
	    }
}
