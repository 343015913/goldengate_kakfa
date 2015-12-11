package com.rogers.cdc.api.serializer;



import java.io.Closeable;
import java.io.Serializable;
import java.util.Map;

import com.rogers.cdc.api.mutations.Mutation;

/**
 *
 * @param <T> Type to be serialized from.
 *
 * A class that implements this interface is expected to have a constructor with no parameter.
 */
//TODO: The name MutationDeserializer is misleading. It doesn't deserialzie into a mutation, but into an Object
//TODO should be a genric
public interface MutationDeserializer extends Closeable {

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
    public Mutation deserialize( String topic, byte[] payload);


    /**
     * Close this deserializer.
     */
    @Override
    public void close();
}