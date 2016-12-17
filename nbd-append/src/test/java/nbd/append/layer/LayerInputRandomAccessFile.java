package nbd.append.layer;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * This class is not to be used in server. Test use only.
 */
public class LayerInputRandomAccessFile implements LayerInput {

  private final RandomAccessFile rand;

  public LayerInputRandomAccessFile(RandomAccessFile rand) {
    this.rand = rand;
  }

  @Override
  public void seek(long position) throws IOException {
    rand.seek(position);
  }

  @Override
  public long getPosition() throws IOException {
    return rand.getFilePointer();
  }

  @Override
  public void close() throws IOException {
    rand.close();
  }

  @Override
  public long length() throws IOException {
    return rand.length();
  }

  @Override
  public byte readByte() throws IOException {
    return rand.readByte();
  }

  @Override
  public void read(byte[] buf, int offset, int length) throws IOException {
    rand.read(buf, offset, length);
  }

}
