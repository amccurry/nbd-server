package nbd.append.layer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.IOUtils;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nbd.append.layer.Layer.CacheContext;
import nbd.append.layer.Layer.Reader;
import nbd.append.layer.Layer.ReaderLayerInput;
import nbd.append.layer.Layer.Writer;
import nbd.append.layer.Layer.WriterLayerOutput;

public abstract class LayerManager implements LayerStorage {

  private static final Logger LOG = LoggerFactory.getLogger(LayerManager.class);

  static class ReaderState {
    final List<Reader> readers;
    final int blockSize;

    ReaderState(int blockSize, List<Reader> readers) {
      this.blockSize = blockSize;
      this.readers = readers;
    }

    void readBlock(int blockId, byte[] buf, int off) throws IOException {
      for (Reader reader : readers) {
        if (reader.readBlock(blockId, buf, off)) {
          return;
        }
      }
      Arrays.fill(buf, off, off + blockSize, (byte) 0);
    }
  }

  private final Object lock = new Object();
  private final int blockSize;
  private final int maxCacheMemory;
  private final AtomicReference<ReaderState> readerState = new AtomicReference<ReaderState>();
  private CacheContext currentWriter;

  public LayerManager(int blockSize, int maxCacheMemory) throws IOException {
    this.blockSize = blockSize;
    this.maxCacheMemory = maxCacheMemory;
    readerState.set(new ReaderState(blockSize, null));
  }

  @Override
  public void open() throws IOException {
    refeshReaders();
  }

  @Override
  public void close() throws IOException {
    tryToFlush();
  }

  @Override
  public void flush() throws IOException {
    tryToFlush();
  }

  @Override
  public void compact() throws IOException {
    synchronized (lock) {
      tryToFlush();
      refeshReaders();
      compactInternal();
    }
  }

  @Override
  public void trim(int startingBlockId, int count) throws IOException {
    tryToFlush();
    addTrimLayer(startingBlockId, count);
  }

  @Override
  public void readBlock(int blockId, byte[] block) throws IOException {
    readBlock(blockId, block, 0);
  }

  @Override
  public void readBlock(int blockId, byte[] buf, int off) throws IOException {
    synchronized (lock) {
      refeshReaders();
      ReaderState state = readerState.get();
      state.readBlock(blockId, buf, off);
    }
  }

  @Override
  public void writeBlock(int blockId, byte[] buf, int off) throws IOException {
    synchronized (lock) {
      Writer writer = getWriter(blockId);
      writer.append(blockId, buf, off);
    }
  }

  @Override
  public void writeBlock(int blockId, byte[] block) throws IOException {
    writeBlock(blockId, block, 0);
  }

  private void addTrimLayer(int startingBlockId, int count) throws IOException {
    synchronized (lock) {
      try (WriterLayerOutput output = newWriterLayerOutput()) {
        output.appendEmpty(startingBlockId, count);
      }
    }
  }

  private void tryToFlush() throws IOException {
    synchronized (lock) {
      if (currentWriter != null) {
        LOG.debug("Flushing writer layer {}", currentWriter.getLayerId());
        currentWriter.close();
        currentWriter = null;
      }
    }
  }

  private void compactInternal() throws IOException {
    List<Reader> readers = readerState.get().readers;
    try (WriterLayerOutput output = newWriterLayerOutput()) {
      writeReadersToOutput(readers, output);
    }
    removeOldFiles(readers);
  }

  private void removeOldFiles(List<Reader> readers) throws IOException {
    for (Reader reader : readers) {
      long layerId = reader.getLayerId();
      removeLayer(layerId);
      IOUtils.closeQuietly(reader);
    }
    this.readerState.set(null);
  }

  private void writeReadersToOutput(List<Reader> readers, WriterLayerOutput output) throws IOException {
    RoaringBitmap dataBlocks = new RoaringBitmap();
    RoaringBitmap zeroBlocks = new RoaringBitmap();
    buildBitmaps(readers, dataBlocks, zeroBlocks);

    RoaringBitmap allBlocks = new RoaringBitmap();
    allBlocks.or(zeroBlocks);
    allBlocks.or(dataBlocks);

    byte[] buf = new byte[blockSize];
    for (Integer blockId : allBlocks) {
      if (dataBlocks.contains(blockId)) {
        writeDataFromReader(output, buf, blockId);
      } else {
        output.appendEmpty(blockId, 1);
      }
    }
  }

  private void writeDataFromReader(WriterLayerOutput output, byte[] buf, Integer blockId) throws IOException {
    for (Reader reader : readerState.get().readers) {
      if (reader.readBlock(blockId, buf, 0)) {
        output.append(blockId, buf);
        return;
      }
    }
  }

  private static void buildBitmaps(List<Reader> readers, RoaringBitmap dataBlocks, RoaringBitmap zeroBlocks)
      throws IOException {
    for (Reader reader : readers) {
      RoaringBitmap rdb = (RoaringBitmap) reader.getDataBlocks();
      RoaringBitmap clone = rdb.clone();
      clone.andNot(zeroBlocks);
      dataBlocks.or(clone);
      RoaringBitmap edb = (RoaringBitmap) reader.getEmptyBlocks();
      zeroBlocks.or(edb);
    }
  }

  private CacheContext getWriter(int blockId) throws IOException {
    if (currentWriter != null && currentWriter.canAppend(blockId)) {
      LOG.debug("reusing writer current size {}", currentWriter.getCurrentSize());
      return currentWriter;
    } else {
      return currentWriter = newCacheWriter();
    }
  }

  private CacheContext newCacheWriter() throws IOException {
    flush();
    long layerId = getNextLayerId();
    return new CacheContext(layerId, maxCacheMemory, blockSize, syncLayerId -> newWriterLayerOutput(syncLayerId));
  }

  private WriterLayerOutput newWriterLayerOutput() throws IOException {
    long layerId = getNextLayerId();
    return new WriterLayerOutput(layerId, blockSize, newOutput(layerId));
  }

  private WriterLayerOutput newWriterLayerOutput(long syncLayerId) throws IOException {
    return new WriterLayerOutput(syncLayerId, blockSize, newOutput(syncLayerId));
  }

  public int getBlockSize() {
    return blockSize;
  }

  private void refeshReaders() throws IOException {
    synchronized (lock) {
      Map<Long, Reader> readers = index();
      long[] layers = getLayers();
      for (int i = 0; i < layers.length; i++) {
        long layerId = layers[i];
        if (!readers.containsKey(layerId)) {
          LOG.debug("Opening missing reader layer {}", layerId);
          Reader reader = new ReaderLayerInput(openLayer(layerId));
          readers.put(reader.getLayerId(), reader);
        }
      }
      LinkedList<Reader> newReaders = new LinkedList<>(readers.values());
      Collections.sort(newReaders);
      if (currentWriter != null) {
        newReaders.push(currentWriter);
      }
      readerState.set(new ReaderState(blockSize, newReaders));
    }
  }

  private Map<Long, Reader> index() {
    ReaderState state = readerState.get();
    List<Reader> currentReaders = null;
    if (state != null) {
      currentReaders = state.readers;
    }
    Map<Long, Reader> readers = new HashMap<>();
    if (currentReaders != null) {
      for (Reader reader : currentReaders) {
        readers.put(reader.getLayerId(), reader);
      }
    }
    return readers;
  }

  protected abstract long[] getLayers() throws IOException;

  /**
   * Ids are to be returned sequentially in increasing order.
   */
  protected abstract long getNextLayerId() throws IOException;

  protected abstract LayerOutput newOutput(long layerId) throws IOException;

  protected abstract LayerInput openLayer(long layerId) throws IOException;

  protected abstract void removeLayer(long layerId) throws IOException;

}
