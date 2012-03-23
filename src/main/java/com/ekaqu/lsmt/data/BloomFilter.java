package com.ekaqu.lsmt.data;

import com.ekaqu.lsmt.util.ObjectSerializer;
import com.google.common.hash.Funnels;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable BloomFilter
 */
public class BloomFilter implements Writable {
  private com.google.common.hash.BloomFilter<byte[]> bloom;

  public BloomFilter() { /* For writable only */}

  public BloomFilter(int expectedInserts) {
    this.bloom = com.google.common.hash.BloomFilter.create(Funnels.byteArrayFunnel(), expectedInserts);
  }

  public void put(byte[] key) {
    this.bloom.put(key);
  }

  public boolean mightContain(byte[] key) {
    return this.bloom.mightContain(key);
  }

  public void readData(final DataInput input) throws IOException {
    int size = input.readInt();
    byte[] readObj = new byte[size];
    input.readFully(readObj);
    try {
      this.bloom = (com.google.common.hash.BloomFilter<byte[]>) ObjectSerializer.deserialize(readObj);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

  public void writeData(final DataOutput output) throws IOException {
    byte[] obj = ObjectSerializer.serialize(bloom);
    output.writeInt(obj.length);
    output.write(obj);
  }
}
