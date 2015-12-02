package com.rogers.cdc.kafka;

import com.rogers.cdc.api.mutations.Mutation;
import com.rogers.cdc.api.schema.Table;

public class KafkaUtil {
	public static String genericTopic( String db ,  String table){
		 return String.format("%s_%s_generic", db, table);
	}
	public static String genericTopic( Mutation m){
		 return genericTopic( m.getSchemaName() , m.getTableName());
	}
	public static String genericTopic( Table t){
		 return genericTopic( t.getDatabaseName() , t.getName());
	}

}
