package com.rogers.cdc.api.mutations;

/*import com.rogers.goldengate.api.mutations.MutationType;

import com.rogers.goldengate.api.mutations.Row;*/
import com.rogers.cdc.api.mutations.RowMutation;
import com.rogers.cdc.api.schema.*;


public class InsertMutation extends RowMutation {

    public InsertMutation(Table table){
    	this(table, null);
    }
    public InsertMutation(Table table, Row  _row){
    	super(table, _row);
        magicByte = InsertByte; 
    }
	  @Override
	  public MutationType getType(){
	    	return MutationType.INSERT;
	    	
	    }

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
	    }

}
