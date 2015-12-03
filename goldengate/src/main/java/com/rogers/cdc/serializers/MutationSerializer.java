package com.rogers.cdc.serializers;



import java.io.Closeable;
import java.util.Map;

import com.rogers.cdc.api.mutations.Mutation;

//TODO: Could just be org.apache.kafka.common.serialization.Serializer<Mutation>
// I think the initial rational for this class was to make the whole package Kafka independent - but we are using a bunch of the Kafka Connect stuff anyway..
/**
 *
 * A class that implements this interface is expected to have a constructor with no parameter.
 */
public interface MutationSerializer extends Closeable {

    /**
     * Configure this class.
     * @param configs configs in key/value pairs
     * @param isKey whether is for key or value
     */
    public void configure(Map<String, ?> configs);

    /**
     * @param Mutation
     * @return serialized bytes
     */
    public byte[] serialize( Mutation op);


    /**
     * Close this serializer.
     */
    @Override
    public void close();
}

