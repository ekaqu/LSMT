package com.ekaqu.lsmt.data;

import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 *
 */
@Test(groups = "unit")
public class BloomFilterTest {

  public void testReadData() throws Exception {
    BloomFilter bloom = new BloomFilter(10);
    for(int i = 0; i < 10; i++) {
      bloom.put(String.valueOf(i).getBytes());
    }

    ByteArrayOutputStream streamData = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(streamData);
    bloom.writeData(output);
    output.close();

    // read data back from stream
    ByteArrayInputStream streamInput = new ByteArrayInputStream(streamData.toByteArray());
    DataInputStream input = new DataInputStream(streamInput);
    BloomFilter readBloom = new BloomFilter();
    readBloom.readData(input);
    for(int i = 0; i < 10; i++) {
      assertTrue(bloom.mightContain(String.valueOf(i).getBytes()));
    }
    
    assertFalse(bloom.mightContain(String.valueOf(12).getBytes()));
  }
}
