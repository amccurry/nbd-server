package nbd.append.layer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nbd.append.layer.Layer.Reader;
import nbd.append.layer.Layer.Writer;
import nbd.append.layer.Layer.*;
import nbd.append.layer.Layer.CacheContext;

public abstract class LayerManager {

  private final Object lock = new Object();
  private final int blockSize;
  private CacheContext currentWriter;
  private List<Reader> readers;
  private int maxCacheMemory = 64 * 1024;

  public LayerManager(int blockSize) {
    this.blockSize = blockSize;
  }

  public void flush() throws IOException {
    synchronized (lock) {
      if (currentWriter != null) {
        currentWriter.close();
        currentWriter = null;
        readers = openMissingLayers(readers);
      }
    }
  }

  public void readBlock(int blockId, byte[] buf, int off, int len) throws IOException {
    synchronized (lock) {
      if (!currentWriter.readBlock(blockId, buf, off, len)) {
        for (Reader reader : readers) {
          if (reader.readBlock(blockId, buf, off, len)) {
            return;
          }
        }
        Arrays.fill(buf, off, off + len, (byte) 0);
      }
    }
  }

  public void writeBlock(int blockId, byte[] block) throws IOException {
    synchronized (lock) {
      Writer writer = getWriter(blockId);
      writer.append(blockId, block);
    }
  }

  private CacheContext getWriter(int blockId) throws IOException {
    if (currentWriter != null && currentWriter.canAppend(blockId)) {
      return currentWriter;
    } else {
      return currentWriter = newWriter();
    }
  }

  private CacheContext newWriter() throws IOException {
    flush();
    long layerId = getNextLayerId();
    return new CacheContext(layerId, maxCacheMemory, blockSize,
        syncLayerId -> new WriterLayerOutput(syncLayerId, blockSize, newOutput(syncLayerId)));
  }

  public int getBlockSize() {
    return blockSize;
  }

  private List<Reader> openMissingLayers(List<Reader> currentReaders) throws IOException {
    long[] layers = getLayers();
    Map<Long, Reader> readers = index(currentReaders);
    for (int i = 0; i < layers.length; i++) {
      if (!readers.containsKey(layers[i])) {
        Reader reader = new ReaderLayerInput(openLayer(layers[i]));
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

  public void readBlock(int blockId, byte[] block) throws IOException {
    readBlock(blockId, block, 0, block.length);
  }

}
