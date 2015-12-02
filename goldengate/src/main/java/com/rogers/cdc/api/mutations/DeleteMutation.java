package com.rogers.cdc.api.mutations;

//import com.rogers.goldengate.api.mutations.Mutation;
//import com.rogers.goldengate.api.mutations.MutationType;
import com.rogers.cdc.api.schema.*;

public class DeleteMutation extends RowMutation {
	public DeleteMutation(Table table ){
    	this(table, null);
         
    }
	 public DeleteMutation(Table table, Row  _row){
	    	super(table, _row);
	    	magicByte = DeleteByte;
	    }
	  @Override
	    public MutationType getType(){
	    	return MutationType.DELETE;
	    	
	    }   
	    @Override
	    public String toString() {
	        final StringBuilder sb = new StringBuilder();
	        sb.append("DeleteMutation");
	        sb.append("{schema=").append(this.getSchemaName());
	        sb.append(", table=").append(this.getTableName());
	        sb.append("}");
	        return sb.toString();
	        
	    }
}