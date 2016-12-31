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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nbd.append.layer.Layer.CacheContext;
import nbd.append.layer.Layer.Reader;
import nbd.append.layer.Layer.ReaderLayerInput;
import nbd.append.layer.Layer.Writer;
import nbd.append.layer.Layer.WriterLayerOutput;

public abstract class BaseLayerStorage implements LayerStorage {

  private static final Logger LOG = LoggerFactory.getLogger(BaseLayerStorage.class);

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
  private final Thread thread;
  private final AtomicBoolean open = new AtomicBoolean(false);
  private CacheContext currentWriter;

  public BaseLayerStorage(long size, int blockSize, int maxCacheMemory) throws IOException {
    this.blockSize = blockSize;
    this.maxCacheMemory = maxCacheMemory;
    readerState.set(new ReaderState(blockSize, null));
    thread = createCompactionThread();
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
  public void compact(long maxTimeLockTimeNs) throws IOException {
    synchronized (lock) {
      tryToFlush();
      refeshReaders();
      ReaderState state = readerState.get();
      int size = state.readers.size();
      int indexOfReaderToBeCompacted = size - 1;
      Reader reader = state.readers.get(indexOfReaderToBeCompacted);
      LOG.info("Starting compaction of layer {}", reader.getLayerId());
      RoaringBitmap visibleEmptyBlocks = getVisibleEmptyBlocks(indexOfReaderToBeCompacted, state.readers);
      RoaringBitmap visibleDataBlocks = getVisibleDataBlocks(indexOfReaderToBeCompacted, state.readers);
      LOG.info("Visible empty block {}", visibleEmptyBlocks.getCardinality());
      for (Integer blockId : visibleEmptyBlocks) {
        Writer writer = getWriter(blockId);
        writer.appendEmpty(blockId, 1);
      }
      LOG.info("Visible data block {}", visibleDataBlocks.getCardinality());
      byte[] buf = new byte[blockSize];
      for (Integer blockId : visibleDataBlocks) {
        reader.readBlock(blockId, buf, 0);
        writeBlock(blockId, buf);
      }
      releaseOldLayers();
      LOG.info("Finished compaction of layer {}", reader.getLayerId());
    }
  }

  @Override
  public void releaseOldLayers() throws IOException {
    synchronized (lock) {
      refeshReaders();
      ReaderState state = readerState.get();
      List<Reader> readers = new ArrayList<>(state.readers);
      for (int i = 0; i < readers.size(); i++) {
        if (isNotInUse(readers, i)) {
          closeAndRemove(readers.get(i));
        }
      }
      refeshReaders();
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

  private Thread createCompactionThread() {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (open.get()) {
          try {
            executeCompaction();
          } catch (Exception e) {
            LOG.error("Unknown error during compaction.", e);
          }
          try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(5));
          } catch (InterruptedException e) {
            return;
          }
        }
      }

    });
    thread.setDaemon(true);
    thread.setName("Compaction");
    return thread;
  }

  private void executeCompaction() throws Exception {
    LOG.info("Running compaction");
    compact(TimeUnit.MILLISECONDS.toNanos(400));
  }

  @Override
  public void open() throws IOException {
    refeshReaders();
    open.set(true);
    thread.start();
  }

  @Override
  public void close() throws IOException {
    open.set(false);
    tryToFlush();
  }

  @Override
  public void flush() throws IOException {
    tryToFlush();
  }

  private void closeAndRemove(Reader reader) throws IOException {
    LOG.info("layer {} reader {} no longer in use close and remove", reader.getLayerId(), reader);
    reader.close();
    removeLayer(reader.getLayerId());
  }

  private boolean isNotInUse(List<Reader> readers, int readerId) throws IOException {
    RoaringBitmap visibleEmptyBlocks = getVisibleEmptyBlocks(readerId, readers);
    RoaringBitmap visibleDataBlocks = getVisibleDataBlocks(readerId, readers);
    return visibleDataBlocks.getCardinality() == 0 && visibleEmptyBlocks.getCardinality() == 0;
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

  private RoaringBitmap getVisibleBlocks(int index, List<Reader> readers, RoaringBitmap blocks) throws IOException {
    RoaringBitmap visibleBlocks = new RoaringBitmap();
    visibleBlocks.or(blocks);
    for (int i = 0; i < index; i++) {
      Reader r = readers.get(i);
      RoaringBitmap eb = (RoaringBitmap) r.getEmptyBlocks();
      RoaringBitmap db = (RoaringBitmap) r.getDataBlocks();
      visibleBlocks.andNot(eb);
      visibleBlocks.andNot(db);
    }
    return visibleBlocks;
  }

  private RoaringBitmap getVisibleDataBlocks(int index, List<Reader> readers) throws IOException {
    Reader reader = readers.get(index);
    RoaringBitmap blocks = (RoaringBitmap) reader.getDataBlocks();
    return getVisibleBlocks(index, readers, blocks);
  }

  private RoaringBitmap getVisibleEmptyBlocks(int index, List<Reader> readers) throws IOException {
    Reader reader = readers.get(index);
    RoaringBitmap blocks = (RoaringBitmap) reader.getEmptyBlocks();
    return getVisibleBlocks(index, readers, blocks);
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
      long[] layers = getLayers();
      Map<Long, Reader> readers = index(layers);
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

  private Map<Long, Reader> index(long[] layers) {
    ReaderState state = readerState.get();
    List<Reader> currentReaders = null;
    if (state != null) {
      currentReaders = state.readers;
    }
    Map<Long, Reader> readers = new HashMap<>();
    Set<Long> validLayers = new HashSet<>();
    for (long l : layers) {
      validLayers.add(l);
    }
    if (currentReaders != null) {
      for (Reader reader : currentReaders) {
        if (validLayers.contains(reader.getLayerId())) {
          readers.put(reader.getLayerId(), reader);
        }
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
