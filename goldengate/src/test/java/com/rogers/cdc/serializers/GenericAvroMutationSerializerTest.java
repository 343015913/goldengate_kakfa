package com.rogers.goldengate.serializers;

import com.rogers.cdc.api.mutations.*;
import com.rogers.cdc.api.schema.Table;
import com.rogers.cdc.kafka.KafkaUtil;
import com.rogers.cdc.serializers.GenericAvroMutationDeserializer;
import com.rogers.cdc.serializers.GenericAvroMutationSerializer;
import com.rogers.cdc.serializers.MutationDeserializer;
import com.rogers.cdc.serializers.MutationSerializer;






import static org.junit.Assert.assertEquals;






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
	    
	     
	      MutationSerializer serializer = new GenericAvroMutationSerializer();
	      MutationDeserializer deserializer = new GenericAvroMutationDeserializer();

	      //String table = "test_table";
	      //String schema = "test_schema";
	      Table table = new Table("testSchema", "testTable", null, null);
	      String topic = KafkaUtil.genericTopic(table);
	      UpdateMutation updateM  = new UpdateMutation(table,  new Row(update_cols));
	      InsertMutation insertM  = new InsertMutation(table,  new Row(insert_cols));
	      DeleteMutation deleteM  = new DeleteMutation(table);
	      
	      
	      output  = serializer.serialize(topic, updateM);
	      res = deserializer.deserialize(output);
	      
	      /*  assertEquals("Update Mutation Test: table name should be the same",
	    		  table ,  res.getTableName() );
	      assertEquals("Update Mutation Test: table name should be the same",
	    		  schema ,  res.getSchemaName() );*/
	      System.out.println(update_cols[0].getCol().getValue());
	      System.out.println((((UpdateMutation)res).getColumnVal("name")));
	      String bla =  (String) update_cols[0].getCol().getValue(); 
	      String bl2 = (String)(((UpdateMutation)res).getColumnVal("name"));
	      
	      assertEquals("Update Mutation Test: name should be the same",
	    		  (String) update_cols[0].getCol().getValue() ,  (String) (((UpdateMutation)res).getColumnVal("name") )) ;
	      assertEquals("Update Mutation Test: name should be the same",
	    		  (String) update_cols[1].getCol().getValue() ,  (String) (((UpdateMutation)res).getColumnVal("age") )) ;
	      assertEquals("Update Mutation Test: name should be the same",
	    		  (String) update_cols[2].getCol().getValue() ,  (String) (((UpdateMutation)res).getColumnVal("balance")  ));
	      
	      output  = serializer.serialize(topic, insertM);
	      res = deserializer.deserialize(output);
	      
	      
	      /* assertEquals("Insert Mutation Test: table name should be the same",
	    		  table ,  res.getTableName() );
	        assertEquals("Insert Mutation Test: table name should be the same",
	    		  schema ,  res.getSchemaName() );*/
	      assertEquals("Insert Mutation Test: name should be the same",
	    		  (String) insert_cols[0].getCol().getValue() ,  (String)((InsertMutation)res).getColumnVal("name") );
	      assertEquals("Insert Mutation Test: name should be the same",
	    		  (String) insert_cols[1].getCol().getValue() ,  (String)((InsertMutation)res).getColumnVal("age") );
	      assertEquals("Insert Mutation Test: name should be the same",
	    		  (String) insert_cols[2].getCol().getValue() ,  (String)((InsertMutation)res).getColumnVal("balance") );
	      
	      output  = serializer.serialize(topic, deleteM);
	      res = deserializer.deserialize(output);
	      /* assertEquals("Delete Mutation Test: table name should be the same",
	    		  table ,  res.getTableName() );
	      assertEquals("Delete Mutation Test: table name should be the same",
	    		  schema ,  res.getSchemaName() );*/

	    /*  assertEquals("Should support null in serialization and deserialization",
	              null, deserializer.deserialize(topic, serializer.serialize(topic, null)));*/
	  }
	  public static void main(String [ ] args) {
		  AvroSerializerTest tester = new AvroSerializerTest();
		  tester.testSecureSerializerV1();
		}
}
