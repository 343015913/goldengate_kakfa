package com.goldengate.delivery.handler.kafka.util;

import java.security.PublicKey;
import java.security.SecureRandom;
import java.util.Random;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/*
public class DigitalEnvelopeEncrptor {
	final private static Logger logger = LoggerFactory.getLogger(DigitalEnvelopeEncrptor.class);
	
	public static EncryptedMessage encrypt(String message) throws Exception {
		 return encrypt(message.getBytes());
	  }
	 public static EncryptedMessage encrypt(byte[] messageBytes) throws Exception{
	    	// Encrypt the message with a new symmetric key.
	          SymmetricEncryptor symmetricEncryptor = new SymmetricEncryptor();
	          
	          //TODO - Generating a SecretKey for Symmetric Encryption
	      	  //SecretKey senderSecretKey = SymmetricEncrypt.getSecret();
	          byte[] encryptedMessage = symmetricEncryptor.encrypt(messageBytes);
	          PublicKey publicKey;
	          try{  
	             //TODO Get key from KeyStore
	               publicKey = AsymmetricKeyReader.readPublicKey(PUBLIC_KEY_FILENAME);
	          }catch(IOException E){
	        	  
	          }
	          // Encrypt the symmetric key with the public key.
	          AsymmetricEncryptor asymmetricEncriptor = new AsymmetricEncryptor(publicKey);
	          byte[] secretKeyBytes = symmetricEncryptor.getKey().getEncoded();
	          byte[] encryptedSecretKey = asymmetricEncriptor.encrypt(secretKeyBytes);

	          logger.debug("Run  = " + new Random().nextInt(100));
	          logger.debug("Key = " + Hex.encodeHexString(secretKeyBytes));
	          logger.debug("encrypted Secret Key = " + Hex.encodeHexString(encryptedSecretKey));

	          // Encrypt the symmetric key initialization vector with the public key.
	         // byte[] ivBytes = symmetricEncryptor.getInitializationVector().getIV();
	          //byte[] encryptedIV = asymmetricEncriptor.encrypt(ivBytes);
	          //logger.debug("IV = " + Hex.encodeHexString(ivBytes));
	          
	          return new EncryptedMessage(encryptedMessage, encryptedSecretKey, encryptedIV);

		  }
}
class SymmetricEncryptor2
{
  public static final String KEY_ALGORITHM    = "AES";
  public static final String CIPHER_ALGORITHM = "AES/CBC/PKCS5Padding";
  public static final int    KEY_LENGTH_BITS  = 128; 
  public static final int    IV_LENGTH_BYTES  = 16;                           // 256/8 = 32; however, iv must be 16 bytes long (TODO: why?).

 
  final private static Logger logger = LoggerFactory.getLogger(SymmetricEncryptor.class);
  
  private SecretKey _key;
  private IvParameterSpec _iv;
  private Cipher _cipher;

  public SymmetricEncryptor() throws Exception
  {
    _key = generateSymmetricKey(KEY_ALGORITHM);
    _cipher = newCipher(_key, _iv);
      
  }
  
  public byte[] encrypt(byte[] clearMessage) throws Exception
  {
    return _cipher.doFinal(clearMessage);
  }

  public SecretKey getKey()
  {
    return _key;
  }

  private static SecretKey generateSymmetricKey(String algorithm) throws Exception
  {
    //KeyGenerator generator = KeyGenerator.getInstance(KEY_ALGORITHM);
    KeyGenerator generator = KeyGenerator.getInstance(algorithm);
    SecureRandom random = new SecureRandom();
    generator.init(KEY_LENGTH_BITS, random);

    return new SecretKeySpec(Hex.decodeHex("cb024600dce7148b8ddc5d6c111fbd85".toCharArray()),  KEY_ALGORITHM);

  }

  private static Cipher newCipher(SecretKey symmetricKey, IvParameterSpec iv) throws Exception
  {
	
    Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
    //cipher.init(Cipher.ENCRYPT_MODE, symmetricKey, iv);
    cipher.init(Cipher.ENCRYPT_MODE, symmetricKey, cipher.getParameters());// Could just use null for params
    return cipher;
  }
}*/
