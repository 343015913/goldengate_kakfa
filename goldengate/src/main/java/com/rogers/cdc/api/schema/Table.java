package com.rogers.cdc.api.schema;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.codec.binary.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
//TODO: Some of the stuff should be move to a SQLSchema class
public class Table {
	public static String SQL_STRUCT_FIELD_NAME = "val";
	static SchemaBuilder sqlSchemaFor( String name, Schema schema /*, boolean optional*/){
		
		SchemaBuilder bld = SchemaBuilder.struct().optional().name(name).field(SQL_STRUCT_FIELD_NAME,schema);
		/*if (optional){
			bld.optional();
		}*/
		return bld; 
		//return bld.build();	
	}
	static public Struct getSQLSchemaField(Schema schema, String name, Object val  ){
		Schema fieldSchema = schema.field(name).schema();
		return new Struct(fieldSchema).put(Table.SQL_STRUCT_FIELD_NAME, val);	
	}
	static public Object fromSQLSchemaField(Struct struct, String name){
		Struct sqlField = (Struct)struct.get(name);
		return sqlField.get(Table.SQL_STRUCT_FIELD_NAME);
		//return new Struct(fieldSchema).put(Table.SQL_STRUCT_FIELD_NAME, val);	
	}
	public static Schema SQL_INT8_SCHEMA = sqlSchemaFor("sql_int8", SchemaBuilder.INT8_SCHEMA).build();
	public static Schema SQL_INT16_SCHEMA = sqlSchemaFor("sql_int16", SchemaBuilder.INT16_SCHEMA).build();
	public static Schema SQL_INT32_SCHEMA = sqlSchemaFor("sql_int32", SchemaBuilder.INT32_SCHEMA).build();
	public static Schema SQL_INT64_SCHEMA = sqlSchemaFor("sql_int64", SchemaBuilder.INT64_SCHEMA).build();
	public static Schema SQL_FLOAT32_SCHEMA = sqlSchemaFor("sql_float32", SchemaBuilder.FLOAT32_SCHEMA).build();
	public static Schema SQL_FLOAT64_SCHEMA = sqlSchemaFor("sql_float64", SchemaBuilder.FLOAT64_SCHEMA).build();
	public static Schema SQL_BOOLEAN_SCHEMA = sqlSchemaFor("sql_boolean", SchemaBuilder.BOOLEAN_SCHEMA).build();
	public static Schema SQL_STRING_SCHEMA = sqlSchemaFor("sql_string", SchemaBuilder.STRING_SCHEMA).build();
	public static Schema SQL_BYTES_SCHEMA = sqlSchemaFor("sql_bytes", SchemaBuilder.BYTES_SCHEMA).build();
	
	public static Schema SQL_OPTIONAL_INT8_SCHEMA = sqlSchemaFor("sql_optional_int8", SchemaBuilder.OPTIONAL_INT8_SCHEMA).build();
	public static Schema SQL_OPTIONAL_INT16_SCHEMA = sqlSchemaFor("sql_optional_int16", SchemaBuilder.OPTIONAL_INT16_SCHEMA).build();
	public static Schema SQL_OPTIONAL_INT32_SCHEMA = sqlSchemaFor("sql_optional_int32", SchemaBuilder.OPTIONAL_INT32_SCHEMA).build();
	public static Schema SQL_OPTIONAL_INT64_SCHEMA = sqlSchemaFor("sql_optional_int64", SchemaBuilder.OPTIONAL_INT64_SCHEMA).build();
	public static Schema SQL_OPTIONAL_FLOAT32_SCHEMA = sqlSchemaFor("sql_optional_float32", SchemaBuilder.OPTIONAL_FLOAT32_SCHEMA).build();
	public static Schema SQL_OPTIONAL_FLOAT64_SCHEMA = sqlSchemaFor("sql_optional_float64", SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA).build();
	public static Schema SQL_OPTIONAL_BOOLEAN_SCHEMA = sqlSchemaFor("sql_optional_boolean", SchemaBuilder.OPTIONAL_BOOLEAN_SCHEMA).build();
	public static Schema SQL_OPTIONAL_STRING_SCHEMA = sqlSchemaFor("sql_optional_string", SchemaBuilder.OPTIONAL_STRING_SCHEMA).build();
	public static Schema SQL_OPTIONAL_BYTES_SCHEMA = sqlSchemaFor("sql_optional_bytes", SchemaBuilder.OPTIONAL_BYTES_SCHEMA).build();
	
	
	Schema schema;
	//int pkIndex;
	private String name;

	private String database_name;
	//TODO: Probably want to add encoding
	//private final String encoding;
	private List<String> pkColumnNames;
	

	public Table(String d, String name, Schema schema, List<String> pks) {
		this.database_name = d;
		this.name = name;
		//this.encoding = encoding;
		this.schema = schema;

		if ( pks == null )
			pks = new ArrayList<String>();

		this.setPKList(pks);

	}

	public Schema getSchema() {
		return schema;
	}

	public String getName() {
		return this.name;
	}


	public String getDatabaseName() {
		return database_name;
	}

	/*public Table copy() {
		//TODO: deep copy of schema?
	}*/

	public void rename(String tableName) {
		this.name = tableName;
	}

	
	public String fullName() {
		return "`" + this.database_name + "`." + this.name + "`";
	}

	public void setDatabaseName(String database) {
		this.database_name = database;
	}


	public List<String> getPKList() {
		return this.pkColumnNames;
	}


	public void setPKList(List<String> pkColumnNames) {
		this.pkColumnNames = pkColumnNames;
	}
}