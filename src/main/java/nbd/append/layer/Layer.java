package nbd.append.layer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;

import org.roaringbitmap.ImmutableBitmapDataProvider;
import org.roaringbitmap.RoaringBitmap;

import com.google.common.collect.MapMaker;

public class Layer {

  private static final byte[] HEADER = new byte[] { 'F', 'L', '0', '1' };

  private Layer() {
  }

  public static interface Reader extends Closeable, Comparable<Reader> {
    long getLayerId();

    boolean readBlock(int blockId, byte[] buf, int offset, int length) throws IOException;

    @Override
    default int compareTo(Reader o) {
      return Long.compare(o.getLayerId(), getLayerId());
    }
  }

  public static class ReaderLayerInput implements Reader {

    private final ImmutableBitmapDataProvider bitmap;
    private final LayerInput input;
    private final long layerId;
    private final int blockSize;
    private final int headerLength;
    private final int cardinality;

    public ReaderLayerInput(LayerInput input) throws IOException {
      checkHeader(input);
      this.input = input;
      layerId = input.readLong(4);
      blockSize = input.readInt(4 + 8);
      headerLength = 4 + 8 + 4;
      long position = input.readLong(input.length() - 8);
      RoaringBitmap roaringBitmap = new RoaringBitmap();
      roaringBitmap.deserialize(input.getDataInput(position));
      bitmap = (ImmutableBitmapDataProvider) roaringBitmap;
      cardinality = bitmap.getCardinality();
    }

    @Override
    public void close() throws IOException {
      input.close();
    }

    @Override
    public long getLayerId() {
      return layerId;
    }

    @Override
    public boolean readBlock(int blockId, byte[] buf, int offset, int length) throws IOException {
      if (bitmap.contains(blockId)) {
        int numberOfBlockIntoTheDataFile = findBitmapOffset(blockId);
        long pos = ((long) numberOfBlockIntoTheDataFile * (long) blockSize) + (long) headerLength;
        input.read(pos, buf, offset, length);
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

    private void checkHeader(LayerInput inputReader) throws IOException {
      byte[] buf = new byte[4];
      inputReader.read(0, buf, 0, buf.length);
      if (!Arrays.equals(buf, HEADER)) {
        throw new IOException("Not a valid layer file.");
      }
    }

  }

  public static interface Writer extends Closeable {

    boolean canAppend(int blockId);

    void append(int blockId, byte[] block) throws IOException;

  }

  public static abstract class WriterBase implements Writer {

    protected final long layerId;
    protected final int blockSize;

    public WriterBase(long layerId, int blockSize) {
      this.layerId = layerId;
      this.blockSize = blockSize;
    }

    protected void checkInputs(int blockId, byte[] block) throws IOException {
      if (!canAppend(blockId)) {
        throw new IOException("Block id " + blockId + " can not append block");
      }
      if (block.length != blockSize) {
        throw new IOException(
            "Block with length " + block.length + " is different than defined block size " + blockSize);
      }
    }
  }

  public static class WriterLayerOutput extends WriterBase {

    private final RoaringBitmap bitmap = new RoaringBitmap();
    private final LayerOutput output;
    private int prevBlockId = -1;

    public WriterLayerOutput(long layerId, int blockSize, LayerOutput output) throws IOException {
      super(layerId, blockSize);
      this.output = output;
      this.output.write(HEADER);
      this.output.writeLong(layerId);
      this.output.writeInt(blockSize);
    }

    @Override
    public boolean canAppend(int blockId) {
      return blockId > prevBlockId;
    }

    @Override
    public void append(int blockId, byte[] block) throws IOException {
      checkInputs(blockId, block);
      bitmap.add(blockId);
      output.write(block, 0, block.length);
      prevBlockId = blockId;
    }

    @Override
    public void close() throws IOException {
      long position = output.getPosition();
      bitmap.serialize(output);
      output.writeLong(position);
      output.close();
    }

  }

  public static interface WriterCallable {
    Writer newWriter(long layerId) throws IOException;
  }

  public static class CacheContext extends WriterBase implements Reader {

    private final int maxMemory;
    private final ConcurrentMap<Integer, byte[]> blockCache;
    private final WriterCallable writerForClosing;

    public CacheContext(long layerId, int maxMemory, int blockSize, WriterCallable writerForClosing) {
      super(layerId, blockSize);
      this.maxMemory = maxMemory;
      this.writerForClosing = writerForClosing;
      this.blockCache = new MapMaker().makeMap();
    }

    @Override
    public void append(int blockId, byte[] block) throws IOException {
      checkInputs(blockId, block);
      blockCache.put(blockId, copy(block));
    }

    private byte[] copy(byte[] block) {
      byte[] buf = new byte[block.length];
      System.arraycopy(block, 0, buf, 0, block.length);
      return buf;
    }

    @Override
    public void close() throws IOException {
      try (Writer writer = writerForClosing.newWriter(layerId)) {
        Set<Integer> blockIds = new TreeSet<Integer>(blockCache.keySet());
        for (Integer blockId : blockIds) {
          writer.append(blockId, blockCache.get(blockId));
        }
      }
    }

    @Override
    public boolean canAppend(int blockId) {
      return !isFull();
    }

    private boolean isFull() {
      return blockCache.size() * blockSize >= maxMemory;
    }

    @Override
    public long getLayerId() {
      return layerId;
    }

    @Override
    public boolean readBlock(int blockId, byte[] buf, int offset, int length) throws IOException {
      byte[] block = blockCache.get(blockId);
      if (block == null) {
        return false;
      }
      System.arraycopy(block, 0, buf, offset, length);
      return true;
    }

  }

}
