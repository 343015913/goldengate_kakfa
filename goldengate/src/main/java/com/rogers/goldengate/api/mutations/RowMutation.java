package com.rogers.goldengate.api.mutations;

public abstract class RowMutation extends Mutation {

	    protected Row row;
	    
	    public RowMutation(String table,String schema,  Row  _row ){
	    	super(table, schema);
	    	row = _row;
	    }
	    public RowMutation(String table,String schema ){
	    	this(table, schema, null);
	    	
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
	    	return row.getColumn(name).getValue();
	    }
	    

}
