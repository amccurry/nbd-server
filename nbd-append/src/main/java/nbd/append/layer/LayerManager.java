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
import java.util.concurrent.atomic.AtomicReference;

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
  public void releaseOldLayers() throws IOException {
    synchronized (lock) {
      tryToFlush();
      refeshReaders();
      ReaderState state = readerState.get();
      List<Reader> readers = new ArrayList<>(state.readers);
      for (int i = 0; i < readers.size(); i++) {
        if (!isInUse(readers, i)) {
          closeAndRemove(readers.get(i));
        }
      }
      refeshReaders();
    }
  }

  private void closeAndRemove(Reader reader) throws IOException {
    LOG.info("layer {} reader {} no longer in use close and remove", reader.getLayerId(), reader);
    reader.close();
    removeLayer(reader.getLayerId());
  }

  private boolean isInUse(List<Reader> readers, int readerId) throws IOException {
    Reader reader = readers.get(readerId);
    RoaringBitmap readerData = new RoaringBitmap();
    orData(reader, readerData);

    RoaringBitmap existingData = new RoaringBitmap();
    for (int i = 0; i < readerId; i++) {
      Reader r = readers.get(i);
      orData(r, existingData);
    }

    int card1 = readerData.getCardinality();
    readerData.and(existingData);
    int card2 = readerData.getCardinality();
    return card1 != card2;
  }

  private void orData(Reader reader, RoaringBitmap readerData) throws IOException {
    RoaringBitmap dataBlocks = (RoaringBitmap) reader.getDataBlocks();
    RoaringBitmap emptyBlocks = (RoaringBitmap) reader.getEmptyBlocks();
    readerData.or(dataBlocks);
    readerData.or(emptyBlocks);
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
