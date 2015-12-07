package com.rogers.cdc.api.mutations;
import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rogers.cdc.api.schema.*;



public abstract class Mutation implements Serializable { 
	final private static Logger logger = LoggerFactory.getLogger(Mutation.class);
      //String tableName; 
      //String schemaName;// That's the DB schema name - not mutation schema 
      long tx_id;  //TODO: figure out how we set it. Do we need it? 
      protected byte magicByte;
      protected final Table table;
      
      public final static byte UnknownByte = 0x0;
 	  public final static byte InsertByte = 0x1;
 	  public final static byte UpdateByte = 0x2;
 	  public final static byte DeleteByte = 0x3;
 	  public final static byte UpdatePKByte = 0x4;
 	 
      Mutation( Table _table){
    	 // tableName = _tableName;
    	  //schemaName = _schemaName;
    	  table = _table;
      }
      @Override
      public boolean equals(Object ob) {
        if (ob == null) return false;
        if (ob.getClass() != getClass()) return false;
        if (ob == this) return true;
        Mutation other = (Mutation)ob;
        if (!this.getTableName().equals(other.getTableName())) return false;
        if (!this.getTableName().equals(other.getTableName())) return false;
        return true;
      }
      public abstract MutationType getType();
      
      public String getTableName(){
    	  return table.getName();
      } 
      // TODO: rename to DatabaseName
      public String getSchemaName(){
    	  return table.getDatabaseName();
      }
      public byte getMagicByte(){
    	  return magicByte; 
      } 
      public  <T extends Mutation> T getMutation(){
    	  return (T) this;
    	  
      }
      public Table getTable(){
    	  return table; 
    	  
      }
      /*public Object pKey(){
    	  table.getPKList();
      }*/
     
      public void validate(){
    	  //TODO: Need to make sure it was initilzied properly. 
          // Should be abstract? 
    	  //Check rows for RowMutation too
      }


    	  
}
