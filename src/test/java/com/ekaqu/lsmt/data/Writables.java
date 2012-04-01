package com.ekaqu.lsmt.data;

import java.io.*;

/**
 *
 */
public class Writables {

  public static byte[] toBytes(final Writable writable) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytes);
    writable.writeData(out);
    return bytes.toByteArray();
  }

  public static void fromBytes(final Writable writable, final byte[] bytes) throws IOException {
    ByteArrayInputStream inBytes = new ByteArrayInputStream(bytes);
    DataInputStream in = new DataInputStream(inBytes);
    writable.readData(in);
  }
}
