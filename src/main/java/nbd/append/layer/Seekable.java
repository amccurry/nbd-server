package nbd.append.layer;

import java.io.IOException;

public interface Seekable {

  void seek(long position) throws IOException;

  long getPosition() throws IOException;

}
