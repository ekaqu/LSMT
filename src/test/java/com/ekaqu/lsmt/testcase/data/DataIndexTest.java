package com.ekaqu.lsmt.testcase.data;

import com.ekaqu.lsmt.data.DataIndex;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import static org.testng.Assert.assertEquals;

/**
 *
 */
@Test(groups = "unit")
public class DataIndexTest {

  public void testReadData() throws Exception {
    DataIndex index = new DataIndex();
    index.add("a", 0);
    index.add("b", 1);
    index.add("c", 2);
    index.add("d", 3);

    // first write data to stream
    ByteArrayOutputStream streamData = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(streamData);
    index.writeData(output);
    output.close();

    // read data back from stream
    ByteArrayInputStream streamInput = new ByteArrayInputStream(streamData.toByteArray());
    DataInputStream input = new DataInputStream(streamInput);
    DataIndex readIndex = new DataIndex();
    readIndex.readData(input);
    assertEquals(readIndex.getPosition("a"), 0);
    assertEquals(readIndex.getPosition("b"), 1);
    assertEquals(readIndex.getPosition("c"), 2);
    assertEquals(readIndex.getPosition("d"), 3);
  }
}
