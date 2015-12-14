package com.rogers.cdc.serializers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.Collections;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import com.rogers.cdc.api.mutations.Column;
import com.rogers.cdc.api.mutations.DeleteMutation;
import com.rogers.cdc.api.mutations.InsertMutation;
import com.rogers.cdc.api.mutations.Mutation;
import com.rogers.cdc.api.mutations.PkUpdateMutation;
import com.rogers.cdc.api.mutations.Row;
import com.rogers.cdc.api.mutations.UpdateMutation;
import com.rogers.cdc.api.schema.Table;
import com.rogers.cdc.api.serializer.MutationDeserializer;
import com.rogers.cdc.api.serializer.MutationSerializer;
import com.rogers.cdc.kafka.KafkaUtil;

//import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class SpecificAvroMutationSerializerTest {
	private  SchemaRegistryClient schemaRegistry;

	private final String SCHEMA_REGISTRY_URL = "http://ec2-52-91-2-237.compute-1.amazonaws.com:8081";
	public SpecificAvroMutationSerializerTest() {
		
	}
	@Test
	public void testSpecificAvroMutationSerializerWithMockRegistry() {
		schemaRegistry = new MockSchemaRegistryClient();
		MutationSerializer serializer = new SpecificAvroMutationSerializer(schemaRegistry);
		MutationDeserializer deserializer = new SpecificAvroMutationDeserializer(schemaRegistry);

		serializer.configure(Collections.singletonMap("schema.registry.url", "http://fake-url"));
		deserializer.configure(Collections.singletonMap("schema.registry.url", "http://fake-url"));
		
		testSpecificAvroMutationSerializerImpl(serializer, deserializer);
		
	}
	//@Test
	public void testSpecificAvroMutationSerializerWithRealRegistry() {
		MutationSerializer serializer = new SpecificAvroMutationSerializer();
		MutationDeserializer deserializer = new SpecificAvroMutationDeserializer();
		
		serializer.configure(Collections.singletonMap("schema.registry.url", SCHEMA_REGISTRY_URL));
		deserializer.configure(Collections.singletonMap("schema.registry.url", SCHEMA_REGISTRY_URL));

		testSpecificAvroMutationSerializerImpl(serializer, deserializer );
		
	}
	//Private
	private void testSpecificAvroMutationSerializerImpl(MutationSerializer serializer, MutationDeserializer deserializer ) {
		
		Schema schema = SchemaBuilder.struct()
				.field("int8", Schema.OPTIONAL_INT8_SCHEMA)
				.field("int16", Schema.OPTIONAL_INT16_SCHEMA)
				.field("int32", Schema.OPTIONAL_INT32_SCHEMA)
				.field("int64", Schema.OPTIONAL_INT64_SCHEMA)
				.field("float32", Schema.OPTIONAL_FLOAT32_SCHEMA)
				.field("float64", Schema.OPTIONAL_FLOAT64_SCHEMA)
				.field("boolean", Schema.OPTIONAL_BOOLEAN_SCHEMA)
				.field("string", Schema.OPTIONAL_STRING_SCHEMA)
				.field("bytes", Schema.OPTIONAL_BYTES_SCHEMA).build();
		Table table = new Table("testSchema", "testTable", schema, null);
		Struct insertRow = new Struct(schema).put("int8", (byte) 12)
				.put("int16", (short) 12).put("int32", 12)
				.put("int64", (long) 12).put("float32", 12.f)
				.put("float64", 12.).put("boolean", true)
				.put("string", "foobar").put("bytes",  ByteBuffer.wrap("foobar".getBytes()) );
		Struct updateRow = new Struct(schema).put("int32", 12)
				.put("float64", 12.).put("string", "foobar");
		Struct pkUpdateRow = new Struct(schema).put("int32", 12);

		UpdateMutation updateM = new UpdateMutation(table,
				Row.fromStruct(updateRow));
		InsertMutation insertM = new InsertMutation(table,
				Row.fromStruct(insertRow));
		DeleteMutation deleteM = new DeleteMutation(table);
		PkUpdateMutation pkUpdateM = new PkUpdateMutation(table,
				Row.fromStruct(pkUpdateRow));

		byte[] output;
		Mutation res;

		
		String topic = KafkaUtil.genericTopic(table);

        try { 
		output = serializer.serialize(topic, insertM);
		res = deserializer.deserialize(topic, output);
		System.out.println("Insert: " + insertM);
		assertEquals("Insert Mutation: Results should be the same as orignal",insertM,res  );
		
		output = serializer.serialize(topic, updateM);
		res = deserializer.deserialize(topic, output);
		System.out.println("updateM: " + updateM);
		assertEquals("Update Mutation: Results should be the same as orignal", updateM, res);
		
		output = serializer.serialize(topic, deleteM);
		res = deserializer.deserialize(topic, output);
		System.out.println("deleteM: " + deleteM);
		assertEquals("Delete Mutation: Results should be the same as orignal", deleteM, res);
		
		output = serializer.serialize(topic, pkUpdateM);
		res = deserializer.deserialize(topic, output);
		System.out.println("pkUpdateM: " + pkUpdateM);
		assertEquals("UpdatePk Mutation: Results should be the same as orignal", pkUpdateM, res);
        }catch (Exception e){
			fail("Unexpecte expception: " + e);
		}

	}
}
