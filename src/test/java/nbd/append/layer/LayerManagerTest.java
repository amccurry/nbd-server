package nbd.append.layer;

import static org.junit.Assert.assertArrayEquals;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.BitSet;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import nbd.append.TestUtils;

public class LayerManagerTest {

  private static final int MAX_BLOCK_ID = 10000;
  private static final int MAX_NUMBER_OF_BLOCKS = 1000;
  private static final int MAX_BLOCK_SIZE = 1024;
  private static final int MAX_CACHE_SIZE = 128 * 1024;
  private static long seed;

  @BeforeClass
  public static void before() {
    seed = TestUtils.getSeed();
  }

  private File root;
  private int blockSize;
  private LayerManager layerManager;
  private byte[] writeBlock;
  private byte[] readBlock;
  private byte[] readBuffer;
  private Random writeRandom;
  private Random readRandom;
  private int maxCacheSize;

  @Before
  public void setup() {
    root = new File("./target/tmp/" + LayerManagerTest.class.getName());
    TestUtils.rmr(root);
    root.mkdirs();

    Random random = new Random(seed);

    blockSize = random.nextInt(MAX_BLOCK_SIZE) + 1;
    maxCacheSize = random.nextInt(MAX_CACHE_SIZE);
    File dir = new File(root, "LayerManager");
    dir.mkdirs();
    layerManager = getLayerManager(blockSize, maxCacheSize, dir);

    writeBlock = new byte[blockSize];
    readBlock = new byte[blockSize];
    readBuffer = new byte[blockSize];
    writeRandom = new Random(seed);
    readRandom = new Random(seed);
  }

  @After
  public void tearDown() throws IOException {
    layerManager.close();
  }

  @Test
  public void test1() throws IOException {
    write(0);
    write(5);
    readAndAssert(0);
    write(2);
    readAndAssert(5);
    readAndAssert(2);
  }

  @Test
  public void test2() throws IOException {
    Random random = new Random(seed);
    long pass = random.nextLong();
    int passes = 50;
    for (int p = 0; p < passes; p++) {
      int numberOfBlocks = random.nextInt(MAX_NUMBER_OF_BLOCKS);
      int maxBlockId = random.nextInt(MAX_BLOCK_ID);
      System.out
          .println("Running pass [" + p + "] numberOfBlocks [" + numberOfBlocks + "] maxBlockId [" + maxBlockId + "]");
      BitSet bitSet = new BitSet();
      try (RandomAccessFile rand = new RandomAccessFile(
          new File(root, "rand-follower-" + pass + "-" + getClass().getName()), "rw")) {
        rand.setLength(maxBlockId * blockSize);
        for (int i = 0; i < numberOfBlocks; i++) {
          int blockId = writeRandom.nextInt(maxBlockId);
          // System.out.println("Writing [" + blockId + "]");
          byte[] bs = write(blockId);
          long pos = blockId * blockSize;
          rand.seek(pos);
          rand.write(bs);
          bitSet.set(blockId);
        }
        for (int blockId = 0; blockId < maxBlockId; blockId++) {
          if (bitSet.get(blockId)) {
            readAndAssert(blockId, rand);
          }
        }
      }
    }
  }

  private byte[] write(int blockId) throws IOException {
    byte[] block = TestUtils.nextBytes(writeRandom, writeBlock);
    layerManager.writeBlock(blockId, block);
    return block;
  }

  private void readAndAssert(int blockId) throws IOException {
    TestUtils.nextBytes(readRandom, readBlock);
    layerManager.readBlock(blockId, readBuffer);
    assertArrayEquals("Seed [" + seed + "]", readBlock, readBuffer);
  }

  private void readAndAssert(int blockId, RandomAccessFile rand) throws IOException {
    long pos = blockId * blockSize;
    rand.seek(pos);
    rand.readFully(readBlock);
    layerManager.readBlock(blockId, readBuffer);
    assertArrayEquals("Seed [" + seed + "]", readBlock, readBuffer);
  }

  private static LayerManager getLayerManager(int blockSize, int maxCacheSize, File dir) {
    return new FileLayerManager(blockSize, maxCacheSize, dir);
  }

}
