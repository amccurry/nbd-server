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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import nbd.append.TestUtils;
import nbd.append.layer.Layer.Reader;
import nbd.append.layer.Layer.Writer;

public class LayerTest {

  private static final int MAX_LAYERS = 25;
  private static final int MAX_INITAL_BLOCK_ID = 1000;
  private static final int MAX_BLOCK_SIZE = 1000;
  private static final int MAX_BLOCK_TO_GENERATE = 10000;
  private static final int MAX_BLOCKS_TO_SKIP = 1000;
  private static long seed;
  private File dir;
  private int blockSize;
  private int layers;
  private int initalBlockId;
  private int numberOfBlocks;

  @BeforeClass
  public static void before() {
    seed = TestUtils.getSeed();
  }

  @Before
  public void setup() {
    dir = new File("./target/tmp/" + LayerTest.class.getName());
    TestUtils.rmr(dir);
    dir.mkdirs();
    Random random = new Random(seed);
    blockSize = random.nextInt(MAX_BLOCK_SIZE);
    layers = random.nextInt(MAX_LAYERS);
    initalBlockId = random.nextInt(random.nextInt(MAX_INITAL_BLOCK_ID));
    numberOfBlocks = random.nextInt(random.nextInt(MAX_BLOCK_TO_GENERATE));
  }

  @Test
  public void basicTest() throws IOException {
    byte[] buf = new byte[blockSize];
    long writeTotal = 0;
    long readTotal = 0;
    {
      Random random = new Random(seed);
      for (int layer = 0; layer < layers; layer++) {
        File file = new File(dir, layer + ".layer");
        writeTotal += writeLayer(layer, file, random, blockSize, buf);
      }
    }
    {
      Random random = new Random(seed);
      for (int layer = 0; layer < layers; layer++) {
        File file = new File(dir, layer + ".layer");
        readTotal += readLayer(layer, file, random, blockSize, buf);
      }
    }
    assertEquals("Seed [" + seed + "]", readTotal, writeTotal);
  }

  private long readLayer(int layerId, File file, Random random, int blockSize, byte[] buf) throws IOException {
    long total = 0;
    byte[] readBuf = new byte[blockSize];
    try (Reader reader = new Layer.ReaderLayerInput(getInputReader(file))) {
      int blockId = initalBlockId;
      for (int i = 0; i < numberOfBlocks; i++) {
        random.nextBytes(buf);
        reader.readBlock(blockId, readBuf, 0);
        assertArrayEquals("Seed [" + seed + "]", readBuf, buf);
        total += buf.length;
        blockId += (random.nextInt(MAX_BLOCKS_TO_SKIP) + 1);
      }
    }
    return total;
  }

  private long writeLayer(long layerId, File file, Random random, int blockSize, byte[] buf) throws IOException {
    long total = 0;
    try (LayerOutput output = FileLayerStorage.toLayerOutput(new FileOutputStream(file))) {
      int blockId = initalBlockId;
      try (Writer writer = new Layer.WriterLayerOutput(layerId, blockSize, output)) {
        for (int i = 0; i < numberOfBlocks; i++) {
          random.nextBytes(buf);
          writer.append(blockId, buf);
          total += buf.length;
          blockId += (random.nextInt(MAX_BLOCKS_TO_SKIP) + 1);
        }
      }
    }
    return total;
  }

  private static LayerInput getInputReader(File file) throws IOException {
    RandomAccessFile rand = new RandomAccessFile(file, "r");
    return FileLayerStorage.toLayerInput(rand);
  }

}
