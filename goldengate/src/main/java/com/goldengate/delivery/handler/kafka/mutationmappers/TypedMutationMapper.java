package com.goldengate.delivery.handler.kafka.mutationmappers;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.goldengate.atg.datasource.meta.ColumnMetaData;
import com.goldengate.atg.datasource.meta.DsType;
import com.goldengate.atg.datasource.meta.TableMetaData;
import com.goldengate.atg.datasource.DsColumn;
import com.rogers.cdc.api.mutations.Column;
import com.rogers.cdc.api.schema.AbstractSQLTypeConverter;
import com.rogers.cdc.api.schema.SQLDataConverter;
import com.rogers.cdc.api.schema.Table;


public class TypedMutationMapper extends AbstractMutationMapper {
	final private static Logger logger = LoggerFactory.getLogger(TypedMutationMapper.class);
	AbstractSQLTypeConverter<DsColumn> typeConvertor;

	@Override
	protected Column convertColumn(DsColumn column, int colType) {
		try {
		   SQLDataConverter.convertFieldValue(typeConvertor, column, colType);
		} catch (IOException e) {
	        logger.warn("Ignoring record because processing failed:", e);
	    } 
		catch (SQLException e) {
			logger.warn("Ignoring record due to SQL error:", e);
	   }
	}
	 @Override
	   protected Table toTable(TableMetaData tb){
		   //TODO
		   String tableName = tb.getTableName().getOriginalShortName().toLowerCase();
		   String databaseName = tb.getTableName().getOriginalSchemaName().toLowerCase();
		   SchemaBuilder builder = SchemaBuilder.struct().name(databaseName + "." + tableName);
		   
		   List<String> pkColumnNames = new ArrayList();
		   setSchema(builder, tb, pkColumnNames);
		   Schema schema = builder.build();
		   return new Table(databaseName, tableName, schema, pkColumnNames );
	   }
	   // Returns a Schema and a list of primary keys 
	   private void setSchema( SchemaBuilder builder, TableMetaData tb, List<String> pkColumnNames){
		   for(ColumnMetaData column : tb.getColumnMetaData()) {
             logger.debug("meta for column = " + column.getColumnName()  );
               if ( column.isKeyCol()){
              	  pkColumnNames.add(column.getColumnName());
              	 
		    	 }
                 addFieldSchema(builder,column );
		    }    
	   }
	   // TODO: copied from the JDBC connector.... should be part of some abstract layer
	   private void addFieldSchema(SchemaBuilder builder, ColumnMetaData column){
		  DsType type =  column.getDataType();
		  int sqlType = type.getJDBCType();
		  String fieldName = column.getColumnName();
		  boolean optional = column.isNullable();
		  int scale = type.getScale();
		  SQLDataConverter.addFieldSchema(builder,sqlType,fieldName, optional, scale); 
		  
		  logger.debug("\t toString = " + type.toString());
		  logger.debug("\t getJDBCType = " + type.getJDBCType());
		  logger.debug("\t getPrecision = " + type.getPrecision());
		  logger.debug("\t getScale = " + type.getScale());
		  logger.debug("\t getGGDataType = " + type.getGGDataType());
		  logger.debug("\t getGGDataSubType = " + type.getGGDataSubType());

	   }

}
