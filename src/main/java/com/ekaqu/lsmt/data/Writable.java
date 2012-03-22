package com.ekaqu.lsmt.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Allows reading and writing data
 */
public interface Writable {
  void readData(DataInput input) throws IOException;
  void writeData(DataOutput output) throws IOException;
}
