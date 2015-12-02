package com.rogers.cdc.api.mutations;

import com.rogers.cdc.api.schema.*;

public class PkUpdateMutation extends Mutation {
	public PkUpdateMutation(Table table){
		super(table);
        magicByte = UpdatePKByte; 
    	
		assert false : "not implimented";

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
