package com.rogers.goldengate.api.mutations;

import java.util.List;

public class DeleteMutation extends Mutation {
	public DeleteMutation(String table, String schema ){
    	super(table, schema);
        magicByte = DeleteByte; 
    }
	  @Override
	    public MutationType getType(){
	    	return MutationType.DELETE;
	    	
	    	
	    }
	    
	    @Override
	    public String toString() {
	        final StringBuilder sb = new StringBuilder();
	        sb.append("UpdateMutation");
	        sb.append("{schema=").append(schemaName);
	        sb.append(", table=").append(tableName);
	        return sb.toString();
	    }
}