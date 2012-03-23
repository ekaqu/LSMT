package com.ekaqu.lsmt.util;

import java.io.*;

/**
 *
 */
public class ObjectSerializer {

  /**
   * Serializes an Object that implements Serializable
   */
  public static byte[] serialize(Serializable obj) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    ObjectOutputStream output = new ObjectOutputStream(bytes);
    output.writeObject(obj);
    output.flush();
    output.close();
    return bytes.toByteArray();
  }
  
  public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
    ByteArrayInputStream objBytes = new ByteArrayInputStream(bytes);
    ObjectInputStream input = new ObjectInputStream(objBytes);
    try {
      return input.readObject();
    } finally {
      input.close();
    }
  }
}
