package com.goldengate.delivery.handler.kafka.util;


import  com.goldengate.delivery.handler.kafka.util.EncryptedMessage;

import java.nio.file.*;
import java.security.*;
import java.security.spec.*;

import javax.crypto.*;
import javax.crypto.spec.*;

import java.util.Random;

import org.apache.commons.codec.binary.Hex;

public class Encryptor {
	  //private static final String PUBLIC_KEY_FILENAME  = "public_key.der";
	  //private static final String PRIVATE_KEY_FILENAME = "private_key.der";
	  private static final String PUBLIC_KEY_FILENAME  = "public_key.der";
	  private static final String PRIVATE_KEY_FILENAME = "private_key.der";
	  private static final String SECRET_KEY_ALGORITHM = "AES";
	  public static final int SECRET_KEY_LENGTH_BITS = 1024;
	  private static final String PRIVATE_KEY_HEX_STRING = "30820278020100300d06092a864886f70d0101010500048202623082025e02010002818100c3c501301d5e2c0320b74a1eb7f24a1fd2a2c2051f40a66c0ebca16916f1a1a9df859ac52b01f0e0702d05a38612bae087029ec10e228b5486facf5298f0da1d28dbbcd0b596b81e21fa2b296c48fa7641a3d8187c199b13a42ee96c64445bab92c673a9594185de9049a98e00e6bfbd3fbc54c680a74b18f7b222f44c812475020301000102818100a2eab08964f738b345a7a2e41b7a639b4604326866d0bb6e537940ee1eace937600f64744ecd27b2ef475858f43b640f73eb8747ebc66da2e34d97f909d3edfe3e9d4c34417e91d8a37268d523adcde39bc680a7878642412ccda098c2cc09a2a8854ee2318a0ab973e2fc7894be8fd562f2ba7a026a6f87d77de4e1457afe81024100f12fb18c11319c11ae66e5ca5ed5958313d9ae751381421ba0c84074ccad4fb76cea1a013b25dc2771c780a3e41f5ce7b8f9ccd8eba3e5056cdb50c0a8bf0961024100cfcb32bdbb5951217f5b518a3be20a6bf0ab887334c8f2f7f3b5d47915d67a562ca12624c43baa2646e81c02227d9ad519ac6219ea9715330762224c26440f9502405193f732d031fe7f00856f5e16db99599fa2365f053ce8365e18bdac83fa6f0734c0ae11128788c292ba8f296024b790ed4118e79a347267765d6c1fee33c7a1024100b75442af4cc4efa48b35a94a39ad239eba16cceb3fedef17be28758e632af882611bc88875ad626024fd1200fc272f5cc62ae5de91afbc5f6a2b35b153ad86c50241009487945cf01bdf78cb0d183b6b5005c30981364fa142b17fe372a123dcfc55bb897371e5624150bdd728e6fdc2bb6393a24d7505bda2f06b52f64f52858e3a9d";

	  
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
         /* RSAPrivateKey priv = (RSAPrivateKey) symmetricEncryptor.getKey().getPrivate();
          System.out.println(priv.getModulus());
          System.out.println(priv.getPrivateExponent());*/
          System.out.println("Run  = " + new Random().nextInt(100));
          System.out.println("Key = " + Hex.encodeHexString(secretKeyBytes));
          System.out.println("encrypted Secret Key = " + Hex.encodeHexString(encryptedSecretKey));

          // Encrypt the symmetric key initialization vector with the public key.
          byte[] ivBytes = symmetricEncryptor.getInitializationVector().getIV();
          byte[] encryptedIV = asymmetricEncriptor.encrypt(ivBytes);
          
          return new EncryptedMessage(encryptedMessage, encryptedSecretKey, encryptedIV);

          // <encryptedMessage, encryptedSecretKey, and encryptedIV ARE SENT AND RECEIVED HERE>
	  }
      public static byte[] Decrypt(EncryptedMessage msg)  throws Exception {
    	// Read private key from file.
    	 
          //PrivateKey privateKey = AsymmetricKeyReader.readPrivateKey(PRIVATE_KEY_FILENAME);
          PrivateKey privateKey = AsymmetricKeyReader.readPrivateKey(Hex.decodeHex(PRIVATE_KEY_HEX_STRING.toCharArray()));
    	  AsymmetricDecryptor asymmetricDecryptor = new AsymmetricDecryptor(privateKey);
    	  byte[] receivedSecretKeyBytes ="d".getBytes();
    	  try {
               receivedSecretKeyBytes = asymmetricDecryptor.decrypt(msg.getKey());
          } catch (Exception e1) {		
   	        System.out.println("Error encrpying expecption:" + e1);
		  }
    	  System.out.println("receivedSecretKeyBytes:" + receivedSecretKeyBytes);
		  
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
    System.out.println("Generate Key");
    return new SecretKeySpec(Hex.decodeHex("cb024600dce7148b8ddc5d6c111fbd85".toCharArray()),  KEY_ALGORITHM);
    //return generator.generateKey();
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
    System.out.println("Key = " +  Hex.encodeHexString(keyBytes));
    return readPrivateKey(keyBytes);
  }
  public static PrivateKey readPrivateKey(byte [] keyBytes) throws Exception
  {
   
    KeyFactory keyFactory = newKeyFactory();
    PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
    return keyFactory.generatePrivate(spec);
  }

  public static PublicKey readPublicKey(String filenameDer) throws Exception
  {
    byte[] keyBytes = readAllBytes(filenameDer);
    return readPublicKey(keyBytes);
  }
  public static PublicKey readPublicKey(byte[] keyBytes) throws Exception{
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
