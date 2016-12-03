package nbd.append.layer;

import static org.junit.Assert.assertArrayEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Random;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import nbd.append.TestUtils;
import nbd.append.layer.LayerInput;
import nbd.append.layer.LayerManager;
import nbd.append.layer.LayerOutput;

public class LayerManagerTest {

  private static long seed;

  @BeforeClass
  public static void before() {
    seed = TestUtils.getSeed();
    seed = 6867878143973719929l;
  }

  private File root;
  private int blockSize;
  private LayerManager layerManager;
  private byte[] writeBlock;
  private byte[] readBlock;
  private byte[] readBuffer;
  private Random writeRandom;
  private Random readRandom;

  @Before
  public void setup() {
    root = new File("./target/tmp/" + LayerManagerTest.class.getName());
    TestUtils.rmr(root);
    root.mkdirs();

    blockSize = 1024;
    File dir = new File(root, "LayerManager");
    dir.mkdirs();
    layerManager = getLayerManager(blockSize, dir);

    writeBlock = new byte[blockSize];
    readBlock = new byte[blockSize];
    readBuffer = new byte[blockSize];
    writeRandom = new Random(seed);
    readRandom = new Random(seed);
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
    for (int p = 0; p < 2; p++) {
      int numberOfBlocks = random.nextInt(1000);
      int maxBlockId = random.nextInt(10000);
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

  private static LayerManager getLayerManager(int blockSize, File dir) {
    return new LayerManager(blockSize) {

      @Override
      protected LayerOutput newOutput(long layerId) throws IOException {
        File file = new File(dir, Long.toString(layerId));
        return LayerOutput.toLayerOutput(new FileOutputStream(file));
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
        RandomAccessFile rand = new RandomAccessFile(new File(dir, Long.toString(layerId)), "r");
        return LayerInput.toLayerInput(rand);
      }

      private long[] toLongArray(String[] layers) {
        long[] result = new long[layers.length];
        for (int i = 0; i < layers.length; i++) {
          result[i] = Long.parseLong(layers[i]);
        }
        return result;
      }
    };
  }

}
