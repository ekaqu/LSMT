package com.ekaqu.lsmt.testcase.data;

import com.ekaqu.lsmt.data.Text;
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
public class TextTest {

  public void testReadData() throws Exception {
    String data = "THIS IS my DATA!!!!11aoiansdf";
    Text text = new Text(data);
    
    // first write data to stream
    ByteArrayOutputStream streamData = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(streamData);
    text.writeData(output);
    output.close();
    
    // read data back from stream
    ByteArrayInputStream streamInput = new ByteArrayInputStream(streamData.toByteArray());
    DataInputStream input = new DataInputStream(streamInput);
    Text readText = Text.create(input);
    assertEquals(readText.toString(), data, "Read data does not match");
  }
}
