package com.ekaqu.lsmt.io;

import com.ekaqu.lsmt.data.protobuf.generated.LSMTProtos;
import com.google.common.io.Closeables;
import com.google.protobuf.ByteString;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 *
 */
public class KeyValueFileTest {
  private LSMTProtos.KeyValue value = null;
  private ByteArrayOutputStream outputStream;

  @BeforeClass(alwaysRun = true)
  public void init() throws IOException {
    outputStream = new ByteArrayOutputStream();
  }

  @Test(groups = "file.writer")
  public void testNewWriter() throws Exception {
    KeyValueFile.Writer writer = new KeyValueFile.Writer(outputStream, 20);

    for(int i = 0; i < 20; i++) {
      LSMTProtos.KeyValue kv = LSMTProtos.KeyValue.newBuilder()
        .setRow(ByteString.copyFromUtf8("row" + i))
        .setQualifier(ByteString.copyFromUtf8("qualifier" + i))
        .setTimestamp(System.currentTimeMillis())
        .setValue(ByteString.copyFromUtf8("value" + i))
        .build();
      writer.append(kv);
      if(i == 10) { // pick a random index to query
        value = kv;
      }
    }

    Closeables.closeQuietly(writer);
  }

  @Test(dependsOnGroups = "file.writer")
  public void testNewReader() throws IOException {
    byte[] data = this.outputStream.toByteArray();
    KeyValueFile.Reader reader = new KeyValueFile.Reader(new ByteArrayInputStream(data), data.length);
    final boolean contains = reader.mightContain(value.getRow().toByteArray());

    assertTrue(contains, "The bloom doesn't contain something that was inserted");
  }

  @Test(dependsOnGroups = "file.writer")
  public void testNewReaderIterator() throws IOException {
    byte[] data = this.outputStream.toByteArray();
    KeyValueFile.Reader reader = new KeyValueFile.Reader(new ByteArrayInputStream(data), data.length);
    int count = 0;
    for(LSMTProtos.KeyValue kv : reader) {
      System.out.println(kv);
      count++;
    }
    assertEquals(count, 20, "Should have 20 elements");
  }

  @Test(dependsOnGroups = "file.writer")
  public void readStream() throws IOException {
    byte[] data = this.outputStream.toByteArray();
    ByteArrayInputStream inputStream = new ByteArrayInputStream(data);

    LSMTProtos.KeyValue kv = LSMTProtos.KeyValue.parseFrom(inputStream);
  }

  @Test(dependsOnGroups = "file.writer")
  public void readDataFormat() throws IOException {
    byte[] bytes = this.outputStream.toByteArray();
    ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);

    LSMTProtos.DataFormat data = LSMTProtos.DataFormat.parseFrom(inputStream);
    System.out.println(data);
  }
}
