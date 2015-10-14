package com.rogers.goldengate.api.mutations;

import java.io.Serializable;
import java.util.Map;

//import java.io.Serializable;
 /*class ColumnMetadata{
	String name;
	String type;
	Boolean isPrimaryKey; 
	ColumnMetadata(){
		
	}
	
}*/
//TODO: Do we really need this class?
public class Column implements Serializable{
	 // ColumnMetadata metadata;
	  public Serializable value;
	  public String name;
	  
	  public Serializable getValue() {
		return value;
	}
	public void setValue(Serializable value) {
		this.value = value;
	}
	
	  //public String type; 
	  public Column(Serializable v){
		  value = v;
	  }
	  @Override
	    public boolean equals(Object ob) {
	        if (ob == null) return false;
	        if (ob.getClass() != getClass()) return false;
	        if (ob == this) return true;
	 
	  	   Column other = (Column)ob;
	  	   return this.value.equals(other.getValue());
	  	  
	    }

      
}
