package nbd.append.layer;

import java.io.DataInput;
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
  public void read(long position, byte[] buf, int offset, int length) throws IOException {
    seek(position);
    rand.readFully(buf, offset, length);
  }

  @Override
  public long getPosition() throws IOException {
    return rand.getFilePointer();
  }

  @Override
  public long readLong(long position) throws IOException {
    rand.seek(position);
    return rand.readLong();
  }

  @Override
  public int readInt(long position) throws IOException {
    rand.seek(position);
    return rand.readInt();
  }

  @Override
  public int read(long position) throws IOException {
    rand.seek(position);
    return rand.read();
  }

  @Override
  public void seek(long pos) throws IOException {
    rand.seek(pos);
  }

  @Override
  public long length() throws IOException {
    return rand.length();
  }

  @Override
  public void close() throws IOException {
    rand.close();
  }

  @Override
  public DataInput getDataInput(long position) throws IOException {
    rand.seek(position);
    return new DataInput() {

      @Override
      public final void readFully(byte[] b) throws IOException {
        rand.readFully(b);
      }

      @Override
      public final void readFully(byte[] b, int off, int len) throws IOException {
        rand.readFully(b, off, len);
      }

      @Override
      public int skipBytes(int n) throws IOException {
        return rand.skipBytes(n);
      }

      @Override
      public final boolean readBoolean() throws IOException {
        return rand.readBoolean();
      }

      @Override
      public final byte readByte() throws IOException {
        return rand.readByte();
      }

      @Override
      public final int readUnsignedByte() throws IOException {
        return rand.readUnsignedByte();
      }

      @Override
      public final short readShort() throws IOException {
        return rand.readShort();
      }

      @Override
      public final int readUnsignedShort() throws IOException {
        return rand.readUnsignedShort();
      }

      @Override
      public final char readChar() throws IOException {
        return rand.readChar();
      }

      @Override
      public final int readInt() throws IOException {
        return rand.readInt();
      }

      @Override
      public final long readLong() throws IOException {
        return rand.readLong();
      }

      @Override
      public final float readFloat() throws IOException {
        return rand.readFloat();
      }

      @Override
      public final double readDouble() throws IOException {
        return rand.readDouble();
      }

      @Override
      public final String readLine() throws IOException {
        return rand.readLine();
      }

      @Override
      public final String readUTF() throws IOException {
        return rand.readUTF();
      }
    };
  }

}
