/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package nbd.append.layer;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.roaringbitmap.ImmutableBitmapDataProvider;
import org.roaringbitmap.RoaringBitmap;

import com.google.common.collect.MapMaker;

public class Layer {

  private static final byte[] HEADER = new byte[] { 'F', 'L', '0', '1' };

  private Layer() {
  }

  public static interface Reader extends Closeable, Comparable<Reader> {
    long getLayerId();

    int getBlockSize();

    boolean readBlock(int blockId, byte[] buf, int offset) throws IOException;

    @Override
    default int compareTo(Reader o) {
      return Long.compare(o.getLayerId(), getLayerId());
    }

    ImmutableBitmapDataProvider getEmptyBlocks() throws IOException;

    ImmutableBitmapDataProvider getDataBlocks() throws IOException;
  }

  public static class ReaderLayerInput implements Reader {

    private final ImmutableBitmapDataProvider dataPresent;
    private final ImmutableBitmapDataProvider zerosPresent;
    private final LayerInput input;
    private final long layerId;
    private final int blockSize;
    private final int headerLength;
    private final int cardinality;

    public ReaderLayerInput(LayerInput input) throws IOException {
      checkHeader(input);
      this.input = input;
      input.seek(4);
      layerId = input.readLong();
      blockSize = input.readInt();
      headerLength = 4 + 8 + 4;
      input.seek(input.length() - 8);
      long position = input.readLong();
      input.seek(position);
      DataInput dataInput = toDataInput(input);
      {
        RoaringBitmap roaringBitmap = new RoaringBitmap();
        roaringBitmap.deserialize(dataInput);
        dataPresent = (ImmutableBitmapDataProvider) roaringBitmap;
      }
      {
        RoaringBitmap roaringBitmap = new RoaringBitmap();
        roaringBitmap.deserialize(dataInput);
        zerosPresent = (ImmutableBitmapDataProvider) roaringBitmap;
      }
      cardinality = dataPresent.getCardinality();
    }

    @Override
    public int getBlockSize() {
      return blockSize;
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
    public boolean readBlock(int blockId, byte[] buf, int offset) throws IOException {
      if (zerosPresent.contains(blockId)) {
        Arrays.fill(buf, offset, offset + buf.length, (byte) 0);
        return true;
      } else if (dataPresent.contains(blockId)) {
        int numberOfBlockIntoTheDataFile = findBitmapOffset(blockId);
        long pos = ((long) numberOfBlockIntoTheDataFile * (long) blockSize) + (long) headerLength;
        input.seek(pos);
        input.read(buf, offset, buf.length);
        return true;
      }
      return false;
    }

    private int findBitmapOffset(int key) {
      int low = 0;
      int high = cardinality - 1;

      while (low <= high) {
        int mid = (low + high) >>> 1;
        int midVal = dataPresent.select(mid);
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
      inputReader.seek(0);
      inputReader.read(buf, 0, buf.length);
      if (!Arrays.equals(buf, HEADER)) {
        throw new IOException("Not a valid layer file.");
      }
    }

    @Override
    public ImmutableBitmapDataProvider getEmptyBlocks() throws IOException {
      return zerosPresent;
    }

    @Override
    public ImmutableBitmapDataProvider getDataBlocks() throws IOException {
      return dataPresent;
    }

  }

  public static interface Writer extends Closeable {

    boolean canAppend(int blockId);

    int getBlockSize();

    default void append(int blockId, byte[] block) throws IOException {
      append(blockId, block, 0);
    }

    void append(int blockId, byte[] buf, int off) throws IOException;

    void appendEmpty(int blockId, int count) throws IOException;

  }

  public static abstract class WriterBase implements Writer {

    protected final long layerId;
    protected final int blockSize;

    public WriterBase(long layerId, int blockSize) {
      this.layerId = layerId;
      this.blockSize = blockSize;
    }

    @Override
    public int getBlockSize() {
      return blockSize;
    }

    protected void checkInputs(int blockId, int blockLength) throws IOException {
      if (!canAppend(blockId)) {
        throw new IOException("Block id " + blockId + " can not append block");
      }
      if (blockLength != blockSize) {
        throw new IOException(
            "Block with length " + blockLength + " is different than defined block size " + blockSize);
      }
    }

    protected boolean isAllZeros(byte[] buf, int off, int length) {
      for (int i = 0; i < buf.length; i++) {
        if (buf[i] != 0) {
          return false;
        }
      }
      return true;
    }
  }

  public static class WriterLayerOutput extends WriterBase {

    private final RoaringBitmap dataPresent = new RoaringBitmap();
    private final RoaringBitmap zerosPresent = new RoaringBitmap();
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
    public void appendEmpty(int blockId, int count) throws IOException {
      checkInputs(blockId, blockSize);
      int end = blockId + count;
      zerosPresent.add((long) blockId, end);
      prevBlockId = end - 1;
    }

    @Override
    public void append(int blockId, byte[] buf, int off) throws IOException {
      checkInputs(blockId, buf.length);
      if (isAllZeros(buf, off, buf.length)) {
        zerosPresent.add(blockId);
      } else {
        dataPresent.add(blockId);
        output.write(buf, off, buf.length);
      }
      prevBlockId = blockId;
    }

    @Override
    public void close() throws IOException {
      long position = output.getPosition();
      DataOutput dataOutput = toDataOutput(output);
      dataPresent.serialize(dataOutput);
      zerosPresent.serialize(dataOutput);
      output.writeLong(position);
      output.close();
    }

  }

  public static interface WriterCallable {
    Writer newWriter(long layerId) throws IOException;
  }

  public static class CacheContext extends WriterBase implements Reader {

    private final RoaringBitmap dataPresent = new RoaringBitmap();
    private final RoaringBitmap zerosPresent = new RoaringBitmap();
    private final int maxMemory;
    private final ConcurrentMap<Integer, byte[]> blockCache;
    private final WriterCallable writerForClosing;
    private final AtomicInteger size = new AtomicInteger();

    public CacheContext(long layerId, int maxMemory, int blockSize, WriterCallable writerForClosing) {
      super(layerId, blockSize);
      this.maxMemory = maxMemory;
      this.writerForClosing = writerForClosing;
      this.blockCache = new MapMaker().makeMap();
    }

    @Override
    public void append(int blockId, byte[] buf, int off) throws IOException {
      checkInputs(blockId, buf.length);
      if (blockCache.put(blockId, copy(buf, off)) == null) {
        size.incrementAndGet();
      }
      dataPresent.add(blockId);
      if (zerosPresent.contains(blockId)) {
        zerosPresent.flip(blockId);
      }
    }

    private byte[] copy(byte[] bs, int off) {
      byte[] buf = new byte[blockSize];
      System.arraycopy(bs, off, buf, 0, blockSize);
      return buf;
    }

    @Override
    public void close() throws IOException {
      try (Writer writer = writerForClosing.newWriter(layerId)) {
        RoaringBitmap blockIds = new RoaringBitmap();
        blockIds.or(dataPresent);
        blockIds.or(zerosPresent);
        for (Integer blockId : blockIds) {
          if (zerosPresent.contains(blockId)) {
            writer.appendEmpty(blockId, 1);
          } else {
            writer.append(blockId, blockCache.get(blockId));
          }
        }
      }
    }

    @Override
    public boolean canAppend(int blockId) {
      return !isFull();
    }

    private boolean isFull() {
      return size.get() * blockSize >= maxMemory;
    }

    @Override
    public long getLayerId() {
      return layerId;
    }

    @Override
    public boolean readBlock(int blockId, byte[] buf, int offset) throws IOException {
      if (zerosPresent.contains(blockId)) {
        Arrays.fill(buf, offset, offset + blockSize, (byte) 0);
        return true;
      }
      byte[] block = blockCache.get(blockId);
      if (block == null) {
        return false;
      }
      System.arraycopy(block, 0, buf, offset, blockSize);
      return true;
    }

    public long getCurrentSize() {
      return blockCache.size() * blockSize;
    }

    @Override
    public void appendEmpty(int blockId, int count) throws IOException {
      zerosPresent.add(blockId);
      if (dataPresent.contains(blockId)) {
        dataPresent.flip(blockId);
      }
    }

    @Override
    public ImmutableBitmapDataProvider getEmptyBlocks() throws IOException {
      return zerosPresent;
    }

    @Override
    public ImmutableBitmapDataProvider getDataBlocks() throws IOException {
      return dataPresent;
    }

  }

  public static DataInput toDataInput(LayerInput input) {
    return new DataInputStream(new InputStream() {
      @Override
      public int read() throws IOException {
        return (0xFF) & input.readByte();
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        input.read(b, off, len);
        return len;
      }
    });
  }

  public static DataOutput toDataOutput(LayerOutput output) {
    return new DataOutputStream(new OutputStream() {

      @Override
      public void write(int b) throws IOException {
        output.writeByte((byte) b);
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        output.write(b, off, len);
      }
    });
  }

}
