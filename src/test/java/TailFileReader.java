import com.ekaqu.lsmt.data.DataIndex;
import com.ekaqu.lsmt.data.Text;
import com.ekaqu.lsmt.util.Bytes;
import com.google.common.base.Stopwatch;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.testng.annotations.Test;

import java.io.*;
import java.util.concurrent.TimeUnit;

import static java.lang.System.out;
import static org.testng.Assert.assertEquals;

/**
 *
 */
@Test(groups = "random")
public class TailFileReader {
  
  public void tailFileReader() throws IOException {
    Stopwatch stopwatch = new Stopwatch().start();
    File f = new File("pom.xml");
    
    // read tail
    BufferedReader input = new BufferedReader(new FileReader(f));
    input.skip(f.length() - "</project>".length());
    String line = input.readLine();
    stopwatch.stop();
    out.printf("Read line ==%s== in %s nanoseconds\n", line, stopwatch.elapsedTime(TimeUnit.NANOSECONDS));
    input.close();
    
    // read full and scan for last line
    stopwatch = new Stopwatch().start();
    input = new BufferedReader(new FileReader(f));
    String lastLine = null;
    while( (line = input.readLine()) != null ) {
//      out.printf("Current Line ==%s==\n", line);
      if (line != null) {
        lastLine = line;
      }
    }
    stopwatch.stop();
    out.printf("Read line ==%s== in %s nanoseconds\n", lastLine, stopwatch.elapsedTime(TimeUnit.NANOSECONDS));
    input.close();
  }
  
  public void writeToFile() throws IOException {
    File f = File.createTempFile("writeToFile", ".text");
    f.deleteOnExit();

    // write file
    DataOutputStream output = new DataOutputStream(new FileOutputStream(f));
    output.writeUTF("This is just a test write");
    output.writeUTF("Will this work properly?");
    output.writeBoolean(true);
    output.close();

    // read file
    DataInputStream input = new DataInputStream(new FileInputStream(f));
    assertEquals(input.readUTF(), "This is just a test write");
    assertEquals(input.readUTF(), "Will this work properly?");
    assertEquals(input.readBoolean(), true);
  }

  public void indexedFile() throws IOException {
    File f = File.createTempFile("writeToFile", ".text");
    f.deleteOnExit();

    // write file
    HashFunction hasher = Hashing.md5();
    
    out.printf("Writing to file %s\n", f);
    DataOutputStream output = new DataOutputStream(new FileOutputStream(f));
    DataIndex index = new DataIndex();
    String key = null;
    Text value = null;
    for(int i = 0; i < 20; i++) {
      Text text = new Text(String.valueOf(System.currentTimeMillis()));
      String hash = hasher.hashString(text.toString()).toString();
      index.add(hash, output.size());
      text.writeData(output);
      if(i == 10) {
        key = hash;
        value = text;
      }
    }
    int indexPosition = output.size();
    index.writeData(output);
    output.writeInt(indexPosition); // where the index starts

    // read file
//    DataInputStream input = new DataInputStream(new FileInputStream(f));  // wont work, FileInputStream doesn't support mark/reset
    DataInputStream input = new DataInputStream(new BufferedInputStream(new FileInputStream(f)));
    input.mark(1024); // no idea what to put here
    // Read in index
    input.skip(f.length() - Bytes.SIZEOF_INT);
    int position = input.readInt();
    assertEquals(position, indexPosition, "Location of the index does not match");
    input.reset();
    input.skip(position);
    DataIndex readIndex = new DataIndex();
    readIndex.readData(input);
    
    long keyPosition = index.getPosition(key);
    input.reset();
    input.skip(keyPosition);
    Text readValue = Text.create(input);
    out.printf("Read %s from file\n", readValue);
    input.close();
    assertEquals(readValue.toString(), value.toString(), "Unable to read the data from the file");
  }
}
