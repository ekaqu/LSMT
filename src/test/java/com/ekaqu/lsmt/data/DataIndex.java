package com.ekaqu.lsmt.data;

import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class DataIndex implements Writable {
  private final Map<String, IndexPair> pairs = Maps.newHashMap();

  public void add(String key, long position) {
    add(new IndexPair(key, position));
  }

  private void add(final IndexPair indexPair) {
    this.pairs.put(indexPair.key, indexPair);
  }
  
  public long getPosition(String key) {
    IndexPair pair = pairs.get(key);
    if(pair == null) {
      return -1;
    }
    return pair.position;
  }

  public void readData(final DataInput input) throws IOException {
    this.pairs.clear();
    int size = input.readInt();
    for( int i = 0; i < size; i++ ) {
      IndexPair pair = new IndexPair();
      pair.readData(input);
      this.pairs.put(pair.key, pair);
    }
  }

  public void writeData(final DataOutput output) throws IOException {
    output.writeInt(this.pairs.size());
    for(IndexPair pair : pairs.values()) {
      pair.writeData(output);
    }
  }

  public static class IndexPair implements Writable {
    private String key;
    private long position;

    public IndexPair(final String key, final long position) {
      this.key = key;
      this.position = position;
    }

    IndexPair() {
      // only for Writable
    }

    public void readData(final DataInput input) throws IOException {
      this.key = input.readUTF();
      this.position = input.readLong();
    }

    public void writeData(final DataOutput output) throws IOException {
      output.writeUTF(key);
      output.writeLong(position);
    }
  }
}
