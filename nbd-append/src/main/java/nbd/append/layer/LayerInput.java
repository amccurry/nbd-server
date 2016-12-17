package nbd.append.layer;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;

public interface LayerInput extends Seekable, Closeable {

  long length() throws IOException;

  long readLong(long position) throws IOException;

  int readInt(long position) throws IOException;

  int read(long position) throws IOException;

  void read(long position, byte[] buf, int offset, int length) throws IOException;

  DataInput getDataInput(long position) throws IOException;

}
