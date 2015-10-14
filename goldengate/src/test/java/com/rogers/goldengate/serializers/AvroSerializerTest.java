package com.rogers.goldengate.serializers;

import com.rogers.goldengate.api.mutations.*;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;

/*import com.rogers.kafka.serializers.KafkaSecureByteArrayDeserializer;
import com.rogers.kafka.serializers.KafkaSecureByteArraySerializer;
import com.rogers.kafka.serializers.KafkaSecureSerializerTest;
*/
import org.junit.Test;

public class AvroSerializerTest {
	
	 
	       
	  // All values are passes as string for V1
	 @Test
	  public void testSecureSerializerV1() {
	      byte[] output;
	      Mutation res;
	      
	      Row.RowVal[] update_cols = {new Row.RowVal("name", new Column("Jon")),
                  new Row.RowVal("age", new Column("28")),
                  new Row.RowVal("balance", new Column("5.23"))
          };
          Row.RowVal[] insert_cols = {new Row.RowVal("name", new Column("Jon")),
               new Row.RowVal("age", new Column("28")),
               new Row.RowVal("balance", new Column("5.23"))
           };
	    
	     
	      MutationSerializer serializer = new AvroMutationSerializer();
	      MutationDeserializer deserializer = new AvroMutationDeserializer();

	      String table = "test_table";
	      String schema = "test_schema";

	      UpdateMutation updateM  = new UpdateMutation(table, schema, new Row(update_cols));
	      InsertMutation insertM  = new InsertMutation(table, schema, new Row(insert_cols));
	      DeleteMutation deleteM  = new DeleteMutation(table, schema);
	      
	      
	      output  = serializer.serialize(updateM);
	      res = deserializer.deserialize(output);
	      
	      assertEquals("Update Mutation Test: table name should be the same",
	    		  table ,  res.getTableName() );
	     /* assertEquals("Update Mutation Test: table name should be the same",
	    		  schema ,  res.getSchemaName() );*/
	      
	      assertEquals("Update Mutation Test: name should be the same",
	    		  (String) update_cols[0].getCol().getValue() ,  (String)((UpdateMutation)res).getColumnVal("name")) ;
	      assertEquals("Update Mutation Test: name should be the same",
	    		  (String) update_cols[1].getCol().getValue() ,  (String)((UpdateMutation)res).getColumnVal("age") );
	      assertEquals("Update Mutation Test: name should be the same",
	    		  (String) update_cols[2].getCol().getValue() ,  (String)((UpdateMutation)res).getColumnVal("balance") );
	      
	      output  = serializer.serialize(insertM);
	      res = deserializer.deserialize(output);
	      
	      
	      assertEquals("Insert Mutation Test: table name should be the same",
	    		  table ,  res.getTableName() );
	      /*  assertEquals("Insert Mutation Test: table name should be the same",
	    		  schema ,  res.getSchemaName() );*/
	      assertEquals("Insert Mutation Test: name should be the same",
	    		  (String) insert_cols[0].getCol().getValue() ,  (String)((InsertMutation)res).getColumnVal("name") );
	      assertEquals("Insert Mutation Test: name should be the same",
	    		  (String) insert_cols[1].getCol().getValue() ,  (String)((InsertMutation)res).getColumnVal("age") );
	      assertEquals("Insert Mutation Test: name should be the same",
	    		  (String) insert_cols[2].getCol().getValue() ,  (String)((InsertMutation)res).getColumnVal("balance") );
	      
	      output  = serializer.serialize(deleteM);
	      res = deserializer.deserialize(output);
	      assertEquals("Delete Mutation Test: table name should be the same",
	    		  table ,  res.getTableName() );
	     /* assertEquals("Delete Mutation Test: table name should be the same",
	    		  schema ,  res.getSchemaName() );*/

	    /*  assertEquals("Should support null in serialization and deserialization",
	              null, deserializer.deserialize(topic, serializer.serialize(topic, null)));*/
	  }
	  public static void main(String [ ] args) {
		  AvroSerializerTest tester = new AvroSerializerTest();
		  tester.testSecureSerializerV1();
		}
}
