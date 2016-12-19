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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileLayerManager extends LayerManager {

  private static final Logger LOG = LoggerFactory.getLogger(FileLayerManager.class);

  private final File dir;

  public FileLayerManager(long size, int blockSize, int maxCacheMemory, File dir) throws IOException {
    super(size, blockSize, maxCacheMemory);
    dir.mkdir();
    if (!dir.exists()) {
      throw new IOException("Path [" + dir + "] does not exist.");
    }
    this.dir = dir;
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  @Override
  protected LayerOutput newOutput(long layerId) throws IOException {
    File file = new File(dir, Long.toString(layerId));
    file.getParentFile()
        .mkdirs();
    return toLayerOutput(new FileOutputStream(file));
  }

  @Override
  protected long getNextLayerId() {
    long[] layers = getLayers();
    Arrays.sort(layers);
    if (layers.length == 0) {
      return 1L;
    }
    return layers[layers.length - 1] + 1L;
  }

  @Override
  protected long[] getLayers() {
    return toLongArray(dir.list());
  }

  @Override
  protected LayerInput openLayer(long layerId) throws IOException {
    File file = new File(dir, Long.toString(layerId));
    LOG.info("open layer {}", file);
    RandomAccessFile rand = new RandomAccessFile(file, "r");
    return toLayerInput(rand);
  }

  private long[] toLongArray(String[] layers) {
    if (layers == null) {
      return new long[] {};
    }
    long[] result = new long[layers.length];
    for (int i = 0; i < layers.length; i++) {
      result[i] = Long.parseLong(layers[i]);
    }
    return result;
  }

  @Override
  protected void removeLayer(long layerId) throws IOException {
    File file = new File(dir, Long.toString(layerId));
    LOG.info("removing file {}", file);
    if (!file.delete()) {
      throw new IOException("Can't remove old layer " + layerId);
    }
  }

  public static LayerInput toLayerInput(RandomAccessFile randomAccessFile) {
    return new LayerInputRandomAccessFile(randomAccessFile);
  }

  public static LayerOutput toLayerOutput(OutputStream outputStream) {
    return new LayerOutputStream(outputStream);
  }
}
