package com.goldengate.delivery.handler.kafka.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Should this by part of Encryptor? What if Cipher in Encryptor changes? 
public class EncryptedMessage {

	public static final int    ENC_KEY_LENGTH_BITS  = Encryptor.SECRET_KEY_LENGTH_BITS / 8; 
	public static final int    ENC_IV_LENGTH_BYTES  = Encryptor.SECRET_KEY_LENGTH_BITS / 8; 

	final private static Logger logger = LoggerFactory.getLogger(EncryptedMessage.class);
	
	private byte[] encryptedMessage;
	private byte[] encryptedSecretKey;
	private byte[] encryptedIV;
	
	public EncryptedMessage(byte[] _encryptedMessage, byte[] _encryptedSecretKey, byte[] _encryptedIV){
		assert _encryptedSecretKey.length == ENC_KEY_LENGTH_BITS : "Wrong encryptedSecretKey length:" + _encryptedSecretKey.length;
		assert _encryptedIV.length == ENC_IV_LENGTH_BYTES : "Wrong encryptedIV length: " + _encryptedIV.length ;
		logger.debug("Enc Secret Key = " + _encryptedSecretKey);
		encryptedMessage = _encryptedMessage ;
		encryptedSecretKey = _encryptedSecretKey;
		encryptedIV = _encryptedIV;
	}
	public EncryptedMessage(byte[] payload){
		encryptedSecretKey = Arrays.copyOfRange(payload, 0,ENC_KEY_LENGTH_BITS);
		encryptedIV =  Arrays.copyOfRange(payload, ENC_KEY_LENGTH_BITS,ENC_KEY_LENGTH_BITS + ENC_IV_LENGTH_BYTES);
		encryptedMessage =  Arrays.copyOfRange(payload, ENC_KEY_LENGTH_BITS + ENC_IV_LENGTH_BYTES, payload.length);
	}
	public byte[] getMessage(){
		return  encryptedMessage;
	}
	public byte[] getKey(){
		return  encryptedSecretKey;
	}
	public byte[] getIV(){
		return  encryptedIV;
	}
	public byte[] toByteArray () throws IOException{
		  ByteArrayOutputStream out = new ByteArrayOutputStream();
		  out.write(this.getKey());
		  out.write(this.getIV());
		  out.write(this.getMessage());
		  return out.toByteArray(); 
	}



}
