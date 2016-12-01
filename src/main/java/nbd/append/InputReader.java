package nbd.append;

import java.io.Closeable;
import java.io.IOException;

public interface InputReader extends Closeable {

  long length();

  long readLong(long position) throws IOException;

  int readInt(long position) throws IOException;

  int read(long position) throws IOException;

  void read(long position, byte[] buf, int offset, int length) throws IOException;

}
