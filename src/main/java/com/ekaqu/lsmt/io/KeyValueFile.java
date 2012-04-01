package com.ekaqu.lsmt.io;

import com.ekaqu.lsmt.data.BloomFilter;
import com.ekaqu.lsmt.util.Bytes;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import com.google.common.io.LimitInputStream;
import com.google.protobuf.CodedInputStream;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

import static com.ekaqu.lsmt.data.protobuf.generated.LSMTProtos.DataFormat;
import static com.ekaqu.lsmt.data.protobuf.generated.LSMTProtos.KeyValue;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * File containing KeyValue data and a bloom filter
 */
public class KeyValueFile {

  public static class Writer implements Appendable<KeyValue>, Closeable {
    private final CountingOutputStream out;
    private final DataFormat.Builder data;
    private final BloomFilter bloom;

    public Writer(final OutputStream out, final int expectedInsertions) {
      this.out = new CountingOutputStream(checkNotNull(out));
      this.bloom = new BloomFilter(expectedInsertions);
      this.data = DataFormat.newBuilder();
    }

    public Appendable<KeyValue> append(final KeyValue data) throws IOException {
      this.bloom.put(data.getRow().toByteArray());
      this.data.addData(data);
//      data.writeTo(this.out);
      return this;
    }

    public void close() throws IOException {
      // write KeyValue data
      this.data.build().writeTo(out);

      // write bloom
      long bloomIndex = this.out.getCount();
      DataOutputStream data = new DataOutputStream(this.out);
      bloom.writeData(data);
      data.writeLong(bloomIndex);
      this.out.close();
    }
  }

  public static class Reader implements Iterable<KeyValue> {
    private BloomFilter bloom;
    private DataFormat data;
    private CodedInputStream protoInput;

    public Reader(final InputStream input, final long inputLenght) throws IOException {
      DataInputStream data = new DataInputStream(new BufferedInputStream(input));
      data.mark(1024);
      
      // read bloom
//      data.skip(inputLenght - Bytes.SIZEOF_LONG);
      ByteStreams.skipFully(data, inputLenght - Bytes.SIZEOF_LONG);
      long bloomIndex = data.readLong();
      data.reset();
//      data.skip(bloomIndex);
      ByteStreams.skipFully(data, bloomIndex);
      BloomFilter bf = new BloomFilter();
      bf.readData(data);
      this.bloom = bf;
      
      // create proto's input stream
      data.reset();
      protoInput = CodedInputStream.newInstance(
          new LimitInputStream(data, bloomIndex));
    }

    public boolean mightContain(byte[] key) {
      return this.bloom.mightContain(key);
    }

    public Iterator<KeyValue> iterator() {
      if(this.data == null) {
        try {
          this.data = DataFormat.parseFrom(protoInput);
        } catch (IOException e) {
          Throwables.propagate(e);
        }
      }
      return this.data.getDataList().iterator();
    }
  }

}
