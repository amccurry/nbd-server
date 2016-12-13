package nbd.append.layer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nbd.append.layer.Layer.CacheContext;
import nbd.append.layer.Layer.Reader;
import nbd.append.layer.Layer.ReaderLayerInput;
import nbd.append.layer.Layer.Writer;
import nbd.append.layer.Layer.WriterLayerOutput;

public abstract class LayerManager implements LayerStorage {

  private static final Logger LOG = LoggerFactory.getLogger(LayerManager.class);

  private final Object lock = new Object();
  private final int blockSize;
  private final int maxCacheMemory;
  private CacheContext currentWriter;
  private List<Reader> readers = new ArrayList<>();

  public LayerManager(int blockSize, int maxCacheMemory) {
    this.blockSize = blockSize;
    this.maxCacheMemory = maxCacheMemory;
  }

  @Override
  public void close() throws IOException {
    tryToFlush();
  }

  @Override
  public void flush() throws IOException {
    synchronized (lock) {
      if (tryToFlush()) {
        readers = openMissingLayers(readers);
      }
    }
  }

  @Override
  public void trim(int startingBlockId, int count) throws IOException {
    synchronized (lock) {
      tryToFlush();
      addTrimLayer(startingBlockId, count);
      readers = openMissingLayers(readers);
    }
  }

  private void addTrimLayer(int startingBlockId, int count) throws IOException {
    try (WriterLayerOutput output = newWriterLayerOutput()) {
      output.appendEmpty(startingBlockId, count);
    }
  }

  private boolean tryToFlush() throws IOException {
    synchronized (lock) {
      if (currentWriter != null) {
        LOG.debug("Flushing writer layer {}", currentWriter.getLayerId());
        currentWriter.close();
        currentWriter = null;
        return true;
      }
      return false;
    }
  }

  @Override
  public void readBlock(int blockId, byte[] block) throws IOException {
    readBlock(blockId, block, 0);
  }

  @Override
  public void readBlock(int blockId, byte[] buf, int off) throws IOException {
    synchronized (lock) {
      if (currentWriter == null || !currentWriter.readBlock(blockId, buf, off)) {
        for (Reader reader : readers) {
          if (reader.readBlock(blockId, buf, off)) {
            return;
          }
        }
        Arrays.fill(buf, off, off + blockSize, (byte) 0);
      }
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

  private List<Reader> openMissingLayers(List<Reader> currentReaders) throws IOException {
    long[] layers = getLayers();
    Map<Long, Reader> readers = index(currentReaders);
    for (int i = 0; i < layers.length; i++) {
      long layerId = layers[i];
      if (!readers.containsKey(layerId)) {
        LOG.debug("Opening missing reader layer {}", layerId);
        Reader reader = new ReaderLayerInput(openLayer(layerId));
        readers.put(reader.getLayerId(), reader);
      }
    }
    List<Reader> newReaders = new ArrayList<>(readers.values());
    Collections.sort(newReaders);
    return newReaders;
  }

  private Map<Long, Reader> index(List<Reader> currentReaders) {
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

}
