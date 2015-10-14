package com.rogers.goldengate.api.mutations;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.net.InetAddress;

import java.util.*;

public enum CloumnType {
	 ASCII     (1,  String.class),
     BIGINT    (2,  Long.class),
     BLOB      (3,  ByteBuffer.class),
     BOOLEAN   (4,  Boolean.class),
     COUNTER   (5,  Long.class),
     DECIMAL   (6,  BigDecimal.class),
     DOUBLE    (7,  Double.class),
     FLOAT     (8,  Float.class),
     INET      (16, InetAddress.class),
     INT       (9,  Integer.class),
     TEXT      (10, String.class),
     TIMESTAMP (11, Date.class),
     UUID      (12, UUID.class),
     VARCHAR   (13, String.class),
     VARINT    (14, BigInteger.class),
     TIMEUUID  (15, UUID.class),
     LIST      (32, List.class),
     SET       (34, Set.class),
     MAP       (33, Map.class),
     CUSTOM    (0,  ByteBuffer.class);
	 
	 final Class<?> javaType;
	 private CloumnType(int protocolId, Class<?> javaType) {
         this.javaType = javaType;
     }
}
