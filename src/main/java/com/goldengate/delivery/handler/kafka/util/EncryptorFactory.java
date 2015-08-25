package com.goldengate.delivery.handler.kafka.util;

import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import com.goldengate.delivery.handler.kafka.util.key.ConfigKeyProvider;
import com.goldengate.delivery.handler.kafka.util.key.KeyProvider;
import com.goldengate.delivery.handler.kafka.util.key.TestKeyProvider;

public class EncryptorFactory {

	    public static Encryptor getEncryptor(String name, KeyProvider provider){


	    	Encryptor encryptor; 
	    	switch(name){
	    	case "binary":
	    		encryptor = new  BinaryEncryptor(provider);
	    		break;
	    	case "avro":
	    		encryptor =  new  AvroEncryptor(provider);
	    		break;
	    	default:
	    		encryptor = new  BinaryEncryptor(provider);
	    		break;
	    	}
	    	return encryptor; 
	    }

	
}
