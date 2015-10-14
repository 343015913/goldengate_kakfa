package com.rogers.goldengate.api.mutations;

import java.util.List;

public class PkUpdateMutation extends Mutation {
	public PkUpdateMutation(String table, String schema){
		super(table, schema);
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
	       sb.append("{schema=").append(schemaName);
	       sb.append(", table=").append(tableName);
	       return sb.toString();
	    }
}
