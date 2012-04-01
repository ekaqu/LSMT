package com.ekaqu.lsmt.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Makes Strings writable
 */
public class Text implements Writable{
  private String data = null;
  
  public Text(String data) {
    this.data = data;
  }

  Text() {
    // only for writable
  }

  public static Text create(final DataInput input) throws IOException {
    Text text = new Text();
    text.readData(input);
    return text;
  }

  public void readData(final DataInput input) throws IOException {
    this.data = input.readUTF();
  }

  public void writeData(final DataOutput output) throws IOException {
    output.writeUTF(this.data);
  }

  @Override
  public String toString() {
    return this.data;
  }
}
