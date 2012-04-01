package com.ekaqu.lsmt.io;

import com.ekaqu.lsmt.data.BloomFilter;
import com.ekaqu.lsmt.data.Writables;
import com.ekaqu.lsmt.data.protobuf.generated.LSMTProtos;
import com.google.common.base.Throwables;
import com.google.common.io.CountingOutputStream;
import com.google.protobuf.ByteString;

import java.io.*;
import java.util.Iterator;

import static com.ekaqu.lsmt.data.protobuf.generated.LSMTProtos.DataFormat;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * File containing KeyValue data and a bloom filter
 */
public class KeyValueFile {

  public static class Writer implements Appendable<LSMTProtos.KeyValue>, Closeable {
    private final OutputStream out;
    private final DataFormat.Builder data;
    private final BloomFilter bloom;

    public Writer(final OutputStream out, final int expectedInsertions) {
      this.out = new CountingOutputStream(checkNotNull(out));
      this.bloom = new BloomFilter(expectedInsertions);
      this.data = DataFormat.newBuilder();
    }

    public Appendable<LSMTProtos.KeyValue> append(final LSMTProtos.KeyValue data) throws IOException {
      this.bloom.put(data.getRow().toByteArray());
      this.data.addData(data);
      return this;
    }

    public void close() throws IOException {
      this.data.setBloom(ByteString.copyFrom(Writables.toBytes(this.bloom)));
      this.data.build().writeTo(out);
      this.out.close();
    }
  }

  public static class Reader implements Iterable<LSMTProtos.KeyValue> {
    private BloomFilter bloom;
    private DataFormat data;

    public Reader(final InputStream input, final long inputLenght) throws IOException {
      this.data = DataFormat.parseFrom(input);
    }

    public boolean mightContain(byte[] key) {
      if(this.bloom == null) {
        try {
          loadBloomFilter();
        } catch (IOException e) {
          Throwables.propagate(e);
        }
      }

      return this.bloom.mightContain(key);
    }

    private synchronized void loadBloomFilter() throws IOException {
      this.bloom = new BloomFilter();
      Writables.fromBytes(bloom, this.data.getBloom().toByteArray());
    }

    public Iterator<LSMTProtos.KeyValue> iterator() {
      return this.data.getDataList().iterator();
    }
  }

}
