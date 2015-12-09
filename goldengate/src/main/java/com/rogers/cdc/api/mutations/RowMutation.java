package com.rogers.cdc.api.mutations;

//import com.rogers.cdc.api.mutations.Mutation;
//import com.rogers.cdc.api.mutations.Row;
//import com.rogers.goldengate.api.mutations.RowMutation;

import com.rogers.cdc.api.schema.*;

import org.apache.kafka.connect.data.Struct;
//TODO: Do we really need the Row and Column classes?
//TODO: Should we do a sanity check that a mutation has a row? pKey?


public abstract class RowMutation extends Mutation {

	    protected Row row;
	    //protect Struct struct
	    
	    public RowMutation(Table table,  Row  _row ){
	    	super(table);
	    	row = _row;
	    }
	    public RowMutation(Table table ){
	    	this(table, null);
	    	
	    }
	    @Override
	      public boolean equals(Object ob) {
	    	if (!super.equals(ob)) return false;
	   
	    	RowMutation other = (RowMutation)ob;
	        if (!row.equals(other.row)) return false;
	        return true;
	      }
	    public Row getRow(){
	    	return row;
	    }
	    public Object getColumnVal(String name){
	    	System.out.println(this);
	    	return row.getColumn(name);
	    }
	    

}
