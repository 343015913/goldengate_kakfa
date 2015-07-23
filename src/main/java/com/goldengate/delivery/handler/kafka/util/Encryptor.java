package com.goldengate.delivery.handler.kafka.util;


import  com.goldengate.delivery.handler.kafka.util.EncryptedMessage;

import java.nio.file.*;
import java.security.*;
import java.security.spec.*;

import javax.crypto.*;
import javax.crypto.spec.*;

public class Encryptor {
	  //private static final String PUBLIC_KEY_FILENAME  = "public_key.der";
	  //private static final String PRIVATE_KEY_FILENAME = "private_key.der";
	  private static final String PUBLIC_KEY_FILENAME  = "public_key.der";
	  private static final String PRIVATE_KEY_FILENAME = "private_key.der";
	  private static final String SECRET_KEY_ALGORITHM = "AES";
	  public static final int SECRET_KEY_LENGTH_BITS = 1024;

	  
	  // Test
	  public static void main(String [ ] args)
		{
          try { 
		    String input = "My test: This is a test string to make sure the encyption works. Need to make it longer. Very long. Just to make sure!";
		    EncryptedMessage msg = Encryptor.encrypt(input);
		    byte[] enc_bytes = msg.toByteArray();
		    EncryptedMessage msg2   = new EncryptedMessage(enc_bytes);
		    String output =  Encryptor.DecryptToString(msg2);
		    assert input == output : "Input != Output";
       } catch (Exception e1) {		
    	   System.out.println("Caught expecption:" + e1);
		}
		  
			
		}
	 
	  public static EncryptedMessage encrypt(String message) throws Exception {
		 return encrypt(message.getBytes());
	  }
      public static EncryptedMessage encrypt(byte[] messageBytes) throws Exception{
    	// Encrypt the message with a new symmetric key.
          SymmetricEncryptor symmetricEncryptor = new SymmetricEncryptor();
          byte[] encryptedMessage = symmetricEncryptor.encrypt(messageBytes);
          
          // Read public key from file, for encrypting symmetric key.
          PublicKey publicKey = AsymmetricKeyReader.readPublicKey(PUBLIC_KEY_FILENAME);

          // Encrypt the symmetric key with the public key.
          AsymmetricEncryptor asymmetricEncriptor = new AsymmetricEncryptor(publicKey);
          byte[] secretKeyBytes = symmetricEncryptor.getKey().getEncoded();
          byte[] encryptedSecretKey = asymmetricEncriptor.encrypt(secretKeyBytes);

          // Encrypt the symmetric key initialization vector with the public key.
          byte[] ivBytes = symmetricEncryptor.getInitializationVector().getIV();
          byte[] encryptedIV = asymmetricEncriptor.encrypt(ivBytes);
          
          return new EncryptedMessage(encryptedMessage, encryptedSecretKey, encryptedIV);

          // <encryptedMessage, encryptedSecretKey, and encryptedIV ARE SENT AND RECEIVED HERE>
	  }
      public static byte[] Decrypt(EncryptedMessage msg)  throws Exception {
    	// Read private key from file.
          PrivateKey privateKey = AsymmetricKeyReader.readPrivateKey(PRIVATE_KEY_FILENAME);
          
    	  AsymmetricDecryptor asymmetricDecryptor = new AsymmetricDecryptor(privateKey);
          byte[] receivedSecretKeyBytes = asymmetricDecryptor.decrypt(msg.getKey());
          SecretKey receivedSecretKey = new SecretKeySpec(receivedSecretKeyBytes, SECRET_KEY_ALGORITHM);
          assert receivedSecretKey.getEncoded().length == SECRET_KEY_LENGTH_BITS: "Secret key is " + receivedSecretKey.getEncoded().length + " long, expecting " + SECRET_KEY_LENGTH_BITS;
          // Decrypt the symmetric key initialization vector with the private key.
          byte[] receivedIVBytes = asymmetricDecryptor.decrypt(msg.getIV());
          IvParameterSpec receivedIV = new IvParameterSpec(receivedIVBytes);

          // Decrypt the message.
          SymmetricDecryptor symmetricDecryptor = new SymmetricDecryptor(receivedSecretKey, receivedIV);
          byte[] receivedMessageBytes = symmetricDecryptor.decrypt(msg.getMessage() );

          // The message that was received.
         // System.out.printf("output message: %s\n", receivedMessage);
          return receivedMessageBytes; 
    	  
      }
      public static String DecryptToString(EncryptedMessage msg)  throws Exception {
    	  byte[] receivedMessageBytes = Decrypt(msg);
    	  String receivedMessage = new String(receivedMessageBytes, "UTF8");
          System.out.printf("output message: %s\n", receivedMessage);
          return receivedMessage;
      }

	  
}


class SymmetricEncryptor
{
  public static final String KEY_ALGORITHM    = "AES";
  public static final String CIPHER_ALGORITHM = "AES/CBC/PKCS5Padding";
  //public static final int    KEY_LENGTH_BITS  = 256;
  //TODO Install Java Cryptography Extension to allow 256 bits
  public static final int    KEY_LENGTH_BITS  = 128; 
  public static final int    IV_LENGTH_BYTES  = 16;                           // 256/8 = 32; however, iv must be 16 bytes long (TODO: why?).

  private SecretKey _key;
  private IvParameterSpec _iv;
  private Cipher _cipher;

  public SymmetricEncryptor() throws Exception
  {
    _key = generateSymmetricKey();
    _iv = generateInitializationVector();
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

  public IvParameterSpec getInitializationVector()
  {
    return _iv;
  }

  private static SecretKey generateSymmetricKey() throws Exception
  {
    KeyGenerator generator = KeyGenerator.getInstance(KEY_ALGORITHM);
    SecureRandom random = new SecureRandom();
    generator.init(KEY_LENGTH_BITS, random);
    return generator.generateKey();
  }

  private static IvParameterSpec generateInitializationVector()
  {
    SecureRandom random = new SecureRandom();
    return new IvParameterSpec(random.generateSeed(IV_LENGTH_BYTES));
  }

  private static Cipher newCipher(SecretKey symmetricKey, IvParameterSpec iv) throws Exception
  {
	
    Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
    cipher.init(Cipher.ENCRYPT_MODE, symmetricKey, iv);
    return cipher;
  }
}


//
// Decrypts byte arrays using a symmetric key.
//
class SymmetricDecryptor
{
  public static final String CIPHER_ALGORITHM = "AES/CBC/PKCS5Padding";

  private Cipher _cipher;

  public SymmetricDecryptor(SecretKey key, IvParameterSpec iv) throws Exception
  {
    _cipher = newCipher(key, iv);
  }

  public byte[] decrypt(byte[] encryptedMessage) throws Exception
  {
    return _cipher.doFinal(encryptedMessage);
  }

  private static Cipher newCipher(SecretKey symmetricKey, IvParameterSpec iv) throws Exception
  {
    Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
    cipher.init(Cipher.DECRYPT_MODE, symmetricKey, iv);
    return cipher;
  }
}


//
// Encrypts byte arrays using an asymmetric key pair.
//
class AsymmetricEncryptor
{
  public static final String CIPHER_ALGORITHM = "RSA/ECB/PKCS1Padding";

  private Cipher _cipher;

  public AsymmetricEncryptor(PublicKey publicKey) throws Exception
  {
    _cipher = newCipher(publicKey);
  }

  public byte[] encrypt(byte[] clearMessage) throws Exception
  {
    return _cipher.doFinal(clearMessage);
  }

  private static Cipher newCipher(PublicKey publicKey) throws Exception
  {
    Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
    cipher.init(Cipher.ENCRYPT_MODE, publicKey);
    return cipher;    
  }
}


//
// Decrypts byte arrays using an asymmetric key pair.
//
class AsymmetricDecryptor
{
  public static final String CIPHER_ALGORITHM = "RSA/ECB/PKCS1Padding";

  private Cipher _cipher;

  public AsymmetricDecryptor(PrivateKey privateKey) throws Exception
  {
    _cipher = newCipher(privateKey);
  }

  public byte[] decrypt(byte[] encryptedMessage) throws Exception
  {
    return _cipher.doFinal(encryptedMessage);
  }

  public static Cipher newCipher(PrivateKey privateKey) throws Exception
  {
    Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
    cipher.init(Cipher.DECRYPT_MODE, privateKey);
    return cipher;    
  }
}


//
// Reads public and private keys from a file.
//
class AsymmetricKeyReader
{
  public static final String KEY_ALGORITHM = "RSA";

  public static PrivateKey readPrivateKey(String filenameDer) throws Exception
  {
    byte[] keyBytes = readAllBytes(filenameDer);
    KeyFactory keyFactory = newKeyFactory();
    PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
    return keyFactory.generatePrivate(spec);
  }

  public static PublicKey readPublicKey(String filenameDer) throws Exception
  {
    byte[] keyBytes = readAllBytes(filenameDer);
    KeyFactory keyFactory = newKeyFactory();
    X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);
    return keyFactory.generatePublic(spec);
  }

  private static byte[] readAllBytes(String filename) throws Exception
  {
    return Files.readAllBytes(Paths.get(filename));
  }

  private static KeyFactory newKeyFactory() throws Exception
  {
    return KeyFactory.getInstance(KEY_ALGORITHM);
  }
}
