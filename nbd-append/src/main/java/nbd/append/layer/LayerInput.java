package nbd.append.layer;

import java.io.Closeable;
import java.io.IOException;

public interface LayerInput extends Seekable, Closeable {

  long length() throws IOException;

  default int readInt() throws IOException {
    return ((readByte() & 0xFF) << 24) | ((readByte() & 0xFF) << 16) | ((readByte() & 0xFF) << 8) | (readByte() & 0xFF);
  }

  default long readLong() throws IOException {
    return (((long) readInt()) << 32) | (readInt() & 0xFFFFFFFFL);
  }

  byte readByte() throws IOException;

  void read(byte[] buf, int offset, int length) throws IOException;

}
