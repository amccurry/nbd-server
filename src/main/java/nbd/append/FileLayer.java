package nbd.append;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.roaringbitmap.ImmutableBitmapDataProvider;
import org.roaringbitmap.RoaringBitmap;

public class FileLayer {

  private static final byte[] HEADER = new byte[] { 'F', 'L', '0', '1' };

  public static class Reader implements Closeable {

    private final ImmutableBitmapDataProvider bitmap;
    private final long layerId;
    private final int blockSize;
    private final InputReader inputReader;
    private final int headerLength;
    private final int cardinality;

    public Reader(InputReader inputReader) throws IOException {
      checkHeader(inputReader);
      this.inputReader = inputReader;
      layerId = inputReader.readLong(4);
      blockSize = inputReader.readInt(4 + 8);
      headerLength = 4 + 8 + 4;
      try (ObjectInputStream input = getObjectStream(inputReader)) {
        try {
          bitmap = (ImmutableBitmapDataProvider) input.readObject();
        } catch (ClassNotFoundException e) {
          throw new IOException(e);
        }
      }
      cardinality = bitmap.getCardinality();
    }

    public long getLayerId() {
      return layerId;
    }

    public boolean readBlock(int blockId, byte[] buf, int offset, int length) throws IOException {
      if (bitmap.contains(blockId)) {
        int numberOfBlockIntoTheDataFile = findBitmapOffset(blockId);
        long pos = ((long) numberOfBlockIntoTheDataFile * (long) blockSize) + (long) headerLength;
        inputReader.read(pos, buf, offset, length);
        return true;
      }
      return false;
    }

    private int findBitmapOffset(int key) {
      int low = 0;
      int high = cardinality - 1;

      while (low <= high) {
        int mid = (low + high) >>> 1;
        int midVal = bitmap.select(mid);
        if (midVal < key)
          low = mid + 1;
        else if (midVal > key)
          high = mid - 1;
        else
          return mid; // key found
      }
      return -(low + 1); // key not found.
    }

    private void checkHeader(InputReader inputReader) throws IOException {
      byte[] buf = new byte[4];
      inputReader.read(0, buf, 0, buf.length);
      if (!Arrays.equals(buf, HEADER)) {
        throw new IOException("Not a valid layer file.");
      }
    }

    @Override
    public void close() throws IOException {
      inputReader.close();
    }

    private ObjectInputStream getObjectStream(InputReader inputReader) throws IOException {
      long length = inputReader.length();
      long startingPosition = inputReader.readLong(length - 8);
      return new ObjectInputStream(new InputStream() {
        private long position = startingPosition;

        @Override
        public int read() throws IOException {
          return inputReader.read(position++);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
          try {
            inputReader.read(position, b, off, len);
            return len;
          } finally {
            position += len;
          }
        }
      });
    }

  }

  public static class Writer implements Closeable {

    private final RoaringBitmap bitmap = new RoaringBitmap();
    private final int blockSize;
    private final DataOutputStream outputStream;
    private int prevBlockId = -1;
    private long outputPosition = 0;

    public Writer(long layerId, int blockSize, OutputStream outputStream) throws IOException {
      this.blockSize = blockSize;
      this.outputStream = new DataOutputStream(outputStream);
      this.outputStream.write(HEADER);
      this.outputStream.writeLong(layerId);
      this.outputStream.writeInt(blockSize);
      outputPosition += HEADER.length + 8 + 4;
    }

    public void append(int blockId, byte[] block) throws IOException {
      checkInputs(blockId, block);
      bitmap.add(blockId);
      outputStream.write(block, 0, block.length);
      prevBlockId = blockId;
      outputPosition += block.length;
    }

    @Override
    public void close() throws IOException {
      ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
      objectOutputStream.writeObject(bitmap);
      objectOutputStream.writeLong(outputPosition);
      objectOutputStream.close();
    }

    private void checkInputs(int blockId, byte[] block) throws IOException {
      if (blockId <= prevBlockId) {
        throw new IOException("Block id " + blockId + " is not increasing previous block id " + prevBlockId);
      }
      if (block.length != blockSize) {
        throw new IOException(
            "Block with length " + block.length + " is different than defined block size " + blockSize);
      }
    }
  }

}
