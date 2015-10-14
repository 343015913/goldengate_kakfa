package com.rogers.goldengate.api.mutations;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.io.Serializable;

import com.goldengate.atg.datasource.DsColumn;
import com.goldengate.atg.datasource.adapt.Op;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class Mutation implements Serializable { 
	final private static Logger logger = LoggerFactory.getLogger(Mutation.class);
      String tableName; 
      String schemaName;
      long tx_id;  //TODO: figure out how we set it. Do we need it? 
      protected byte magicByte;
      
      public final static byte UnknownByte = 0x0;
 	  public final static byte InsertByte = 0x1;
 	  public final static byte UpdateByte = 0x2;
 	  public final static byte DeleteByte = 0x3;
 	  public final static byte UpdatePKByte = 0x4;
 	 
      Mutation(String _tableName, String _schemaName){
    	  tableName = _tableName;
    	  schemaName = _schemaName;
      }
      @Override
      public boolean equals(Object ob) {
        if (ob == null) return false;
        if (ob.getClass() != getClass()) return false;
        if (ob == this) return true;
        Mutation other = (Mutation)ob;
        if (!tableName.equals(other.tableName)) return false;
        if (!schemaName.equals(other.schemaName)) return false;
        return true;
      }
      public abstract MutationType getType();
      
      public String getTableName(){
    	  return tableName;
      } 
      public String getSchemaName(){
    	  return schemaName;
      }
      public byte getMagicByte(){
    	  return magicByte; 
      } 
      public  <T extends Mutation> T getMutation(){
    	  return (T) this;
    	  
      }
      // TODO: Move to and OpAdapter Class
      public static Mutation fromOp(Op op){
    	  String tableName = op.getTableName().getOriginalShortName().toLowerCase();
    	  String schemaName = op.getTableName().getOriginalSchemaName();
    	  Row row;
    	  switch(op.getOpType()){
            case DO_INSERT: 
            	 row = createRow(op, false);
       	        return new InsertMutation(tableName, schemaName,  row);
             case  DO_DELETE: 
        	   return new DeleteMutation(tableName, schemaName);
             case DO_UPDATE: 
             case DO_UPDATE_FIELDCOMP: 
             case DO_UPDATE_AC: 
            	row = createRow(op, true);
        	    return new UpdateMutation(tableName, schemaName, row);
             case DO_UPDATE_FIELDCOMP_PK: 
       	        return new PkUpdateMutation(tableName, schemaName);
              default:
       	        //logger.error("The operation type " + op.getOpType() + " on  operation: table=[" + op.getTableName() + "]" + ", op ts=" + op.getTimestamp() + "is not supported");
       	        throw new IllegalArgumentException("KafkaAvroHandler::getMagicByte Unknown operation type");                                                                            
          }
      }
      static Row createRow(Op op, boolean onlyChanged ){
    	  Row row = new Row();
          TableMetaData tbl_meta = op.getTableMeta();; 
            logger.info("create row");
   
		  int i = 0;
		     for(DsColumn column : op) {
                        ColumnMetaData col_meta = tbl_meta.getColumnMeta(i);; 
                        // logger.info("column = " + op.getTableMeta().getColumnName(i) + ", changed = " + column.isChanged() + ", val= " + column.getAfterValue()  );
		    	 if (!onlyChanged || column.isChanged() || col_meta.isKeyCol){
		    		 String name = col_meta.getColumnName(); 
		    		 String str_val = column.getAfterValue();
		    		 Column col = strToColumn(str_val);
                                 //logger.info("Mutation column = " + col.getValue()  );
		    		 row.addColumn(name,col);
		    	 }
                        i++;
		     } 
            logger.info("row: " + row.toString());
		   return row;
      }
      // TODO: Should to Proper type conversaion
      static Column strToColumn(String str){
         // logger.info("strToColumn: str=" + str  );
    	  Column col =  new Column(str);	 
         // logger.info("\t column = " + col.getValue()  );
          return col; 
      }
      
    	  
      
}
