package com.ekaqu.lsmt.data.protobuf;

import com.ekaqu.lsmt.data.protobuf.generated.LSMTProtos;
import com.google.protobuf.ByteString;
import com.google.protobuf.WireFormat;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static com.ekaqu.lsmt.data.protobuf.generated.LSMTProtos.DataFormat;
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

  @Test
  public void readMultiWrite() throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    
    for(int i = 0; i < 10; i++) {
      KeyValue kv = KeyValue.newBuilder()
        .setRow(ByteString.copyFromUtf8("row1"))
        .setQualifier(ByteString.copyFromUtf8("qualifer"))
        .setTimestamp(System.currentTimeMillis())
        .setValue(ByteString.copyFromUtf8("value"))
        .build();
      kv.writeTo(output);
    }

    byte[] data = output.toByteArray();
    ByteArrayInputStream input = new ByteArrayInputStream(data);

    for(int i = 0; i < 10; i++) {
      KeyValue kv = KeyValue.parseFrom(input);
    }
  }
  
  @Test
  public void dataFormat() throws IOException {
    DataFormat.Builder builder = DataFormat.newBuilder();
    for(int i = 0; i < 10; i++) {
      builder.addData(KeyValue.newBuilder()
        .setRow(ByteString.copyFromUtf8("row" + i))
        .setQualifier(ByteString.copyFromUtf8("qualifer" + i))
        .setTimestamp(System.currentTimeMillis())
        .setValue(ByteString.copyFromUtf8("value" + i)));
    }

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    builder.build().writeTo(output);

    ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
    DataFormat data = DataFormat.parseFrom(input);
    System.out.println(data);
  }

  @Test
  public void protoAndNotProto() throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    DataFormat.Builder builder = DataFormat.newBuilder();
    for(int i = 0; i < 10; i++) {
      builder.addData(
          KeyValue.newBuilder()
              .setRow(ByteString.copyFromUtf8("row1"))
              .setQualifier(ByteString.copyFromUtf8("qualifer"))
              .setTimestamp(System.currentTimeMillis())
              .setValue(ByteString.copyFromUtf8("value"))
      );
    }
    builder.build().writeTo(output);
    output.write("Test".getBytes());

    byte[] data = output.toByteArray();
    ByteArrayInputStream input = new ByteArrayInputStream(data);

//    for(int i = 0; i < 10; i++) {
//      KeyValue kv = KeyValue.parseFrom(input);
//    }
    DataFormat.parseFrom(input);
  }
}
