package com.rogers.cdc.api.mutations;

/*import com.rogers.goldengate.api.mutations.MutationType;

import com.rogers.goldengate.api.mutations.Row;*/
import com.rogers.cdc.api.mutations.RowMutation;
import com.rogers.cdc.api.schema.*;


public class InsertMutation extends RowMutation {

   /* public InsertMutation(Table table){
    	this(table, null);
    }*/
    public InsertMutation(Table table, Row  _row){
    	super(table, _row);
        magicByte = InsertByte; 
    }
	  @Override
	  public MutationType getType(){
	    	return MutationType.INSERT;
	    	
	    }
	  @Override 
	  public void validate(){
		  
		  if (! (row.size() == table.getSchema().fields().size())){
			//TODO: The logic bellow makes sense. For some reason, GG somehow sometimes gives Insert Ops with missing collumns. Bug report subbmited
			  
			 // throw new RuntimeException("Insert Mutation failed validation: Cannot have empty columns");
			  
		  }
		  
	  }
	  /*
	    @Override
	    public String toString() {
	        final StringBuilder sb = new StringBuilder();
	        sb.append("InsertMutation");
	        sb.append("{schema=").append(this.getSchemaName());
	        sb.append(", table=").append(this.getTableName());
	        sb.append(", row=[");
	        sb.append("\n").append(row);
	        sb.append("]}");
	        return sb.toString();
	    }*/

}
