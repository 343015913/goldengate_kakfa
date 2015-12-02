package com.rogers.cdc.api.schema;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.codec.binary.StringUtils;
import org.apache.kafka.connect.data.Schema;

public class Table {
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