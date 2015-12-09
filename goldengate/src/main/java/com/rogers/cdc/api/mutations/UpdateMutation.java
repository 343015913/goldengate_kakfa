package com.rogers.cdc.api.mutations;

//import com.rogers.goldengate.api.mutations.MutationType;
//import com.rogers.goldengate.api.mutations.Row;
//import com.rogers.cdc.api.mutations.RowMutation;


import com.rogers.cdc.api.schema.*;

public class UpdateMutation extends RowMutation {

    
    public UpdateMutation(Table table, Row  _row ){
    	super(table, _row);
        magicByte = UpdateByte; 
    }
    public UpdateMutation(Table table ){
    	this(table, null);
    	
    }
    @Override
    public MutationType getType(){
    	return MutationType.UPDATE;	
    }
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("UpdateMutation");
        sb.append("{schema=").append(this.getSchemaName());
        sb.append(", table=").append(this.getTableName());
        sb.append(", row=[");
        sb.append("\n").append(row);
        sb.append("]}");
        return sb.toString();
    }
   
    
}
