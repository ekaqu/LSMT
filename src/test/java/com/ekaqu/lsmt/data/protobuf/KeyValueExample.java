package com.ekaqu.lsmt.data.protobuf;

import com.ekaqu.lsmt.data.protobuf.generated.LSMTProtos;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static com.ekaqu.lsmt.data.protobuf.generated.LSMTProtos.KeyValue;
import static org.testng.Assert.assertEquals;

/**
 *
 */
public class KeyValueExample {
  
  @Test
  public void test() throws IOException {
    KeyValue kv = KeyValue.newBuilder()
      .setRow(ByteString.copyFromUtf8("row1"))
      .setQualifier(ByteString.copyFromUtf8("qualifer"))
      .setTimestamp(System.currentTimeMillis())
      .setValue(ByteString.copyFromUtf8("value"))
      .build();
    
    System.out.println(kv);

    byte[] data = kv.toByteArray();
    System.out.println(data);
    
    KeyValue parsedKV = KeyValue.parseFrom(data);
    System.out.println(parsedKV);
    parsedKV = KeyValue.newBuilder().mergeFrom(data).build();
    System.out.println(parsedKV);

    assertEquals(parsedKV, kv, "KeyValue are different");

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    kv.writeTo(output);
    output.close();
    byte[] outputData = output.toByteArray();
    assertEquals(outputData, data, "byte data is different");
  }
}
