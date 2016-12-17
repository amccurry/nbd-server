package nbd.append;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import nbd.NBDCommand;
import nbd.append.layer.FileLayerManager;
import nbd.append.layer.LayerStorage;

public class AppendStorageTest {

  private long seed;

  @Before
  public void setup() {
    seed = TestUtils.getSeed();
  }

  @Test
  public void readDataFromLayerStorageTest() {
    for (int i = 0; i < 10; i++) {
      runReadTest();
    }
  }

  @Test
  public void validateDataFromLayerStorageTest() throws Exception {
    Random randomSetup = new Random(seed);
    File dir = new File("./target/tmp/" + getClass().getName());
    TestUtils.rmr(dir);
    dir.mkdirs();
    int blockSize = randomSetup.nextInt(1000) + 1;
    int maxCacheMemory = randomSetup.nextInt(1000000);
    int size = 1024 * 1024 * 1024;

    String exportName = "test";
    FileLayerManager layerManager = new FileLayerManager(blockSize, maxCacheMemory, new File(dir, exportName));
    layerManager.open();
    size = (size / blockSize) * blockSize;

    try (AppendStorage storage = new AppendStorage("test", layerManager, blockSize, size)) {

      int bufSize = randomSetup.nextInt(1000) + 1;
      byte[] bufWrite = new byte[bufSize];
      byte[] bufRead = new byte[bufSize];
      int passes = 10;
      {
        Random random = new Random(seed);
        for (int i = 0; i < passes; i++) {
          random.nextBytes(bufWrite);
          int pos = random.nextInt(size / 512) * 512;
          storage.write(bufWrite, pos).call();
          storage.read(bufRead, pos).call();
          assertArrayEquals(bufWrite, bufRead);
        }
      }
      {
        storage.flush().call();
        Random random = new Random(seed);
        for (int i = 0; i < passes; i++) {
          random.nextBytes(bufWrite);
          int pos = random.nextInt(size / 512) * 512;
          NBDCommand command = storage.read(bufRead, pos);
          command.call();
          assertArrayEquals(bufWrite, bufRead);
        }
      }
    }
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
      public void releaseOldLayers() throws IOException {

      }

      @Override
      public void open() throws IOException {

      }

    };
  }
}
