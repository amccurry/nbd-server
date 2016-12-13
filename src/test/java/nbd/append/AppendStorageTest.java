package nbd.append;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import nbd.append.layer.LayerStorage;

public class AppendStorageTest {

  private long seed;

  @Before
  public void setup() {
    seed = TestUtils.getSeed();
    seed = -4007063037374344993l;
  }

  @Test
  public void readDataFromLayerStorageTest() {
    for (int i = 0; i < 10; i++) {
      runReadTest();
    }
  }

  @Test
  public void writeDataFromLayerStorageTest() {
    // @TODO
  }

  private void runReadTest() {
    try {
      Random random = new Random(seed);
      int blockSize = random.nextInt(128) + 1;
      int requestedBlockSize = random.nextInt(128) + 1;
      int numberOfTestBlocks = random.nextInt(128) + 1;

      byte[] resultBlock = new byte[requestedBlockSize];
      List<byte[]> blocks = new ArrayList<>();
      byte[] allResults = new byte[blockSize * numberOfTestBlocks];
      populate(blockSize, numberOfTestBlocks, blocks, allResults);
      LayerStorage layerStorage = getReadLayerStorage(blockSize, numberOfTestBlocks, blocks);
      long max = (numberOfTestBlocks * blockSize) - requestedBlockSize;
      for (long pos = 0; pos < max; pos++) {
        AppendStorage.readDataFromLayerStorage(resultBlock, pos, blockSize, layerStorage);
        checkEquals(pos, allResults, resultBlock);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Seed [" + seed + "] " + e.getMessage());
    }
  }

  private void populate(int blockSize, int numberOfTestBlocks, List<byte[]> blocks, byte[] allResults) {
    int p = 0;
    for (int b = 0; b < numberOfTestBlocks; b++) {
      byte[] block = newBlock((byte) (b + 1), blockSize);
      blocks.add(block);
      System.arraycopy(block, 0, allResults, p, block.length);
      p += block.length;
    }
  }

  private void checkEquals(long pos, byte[] allResults, byte[] resultBlock) {
    for (int i = 0; i < resultBlock.length; i++) {
      int index = (int) (pos + i);
      assertEquals("Seed [" + seed + "]", allResults[index], resultBlock[i]);
    }
  }

  private byte[] newBlock(byte val, int blockSize) {
    byte[] buf = new byte[blockSize];
    Arrays.fill(buf, val);
    return buf;
  }

  private LayerStorage getReadLayerStorage(int blockSize, int numberOfTestBlocks, List<byte[]> blocks) {
    return new LayerStorage() {

      @Override
      public void close() throws IOException {

      }

      @Override
      public void readBlock(int blockId, byte[] block) throws IOException {
        byte[] bs = blocks.get(blockId);
        System.arraycopy(bs, 0, block, 0, blockSize);
      }

      @Override
      public void readBlock(int blockId, byte[] buf, int off) throws IOException {
        byte[] bs = blocks.get(blockId);
        System.arraycopy(bs, 0, buf, off, blockSize);
      }

      @Override
      public void writeBlock(int blockId, byte[] block) throws IOException {

      }

      @Override
      public void writeBlock(int blockId, byte[] buf, int off) throws IOException {

      }

      @Override
      public void flush() throws IOException {

      }

      @Override
      public void trim(int startingBlockId, int count) throws IOException {

      }

      @Override
      public void compact() throws IOException {

      }

      @Override
      public void open() throws IOException {

      }

    };
  }
}
