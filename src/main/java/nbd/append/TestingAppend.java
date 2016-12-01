package nbd.append;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Random;

import nbd.append.FileLayer.Reader;
import nbd.append.FileLayer.Writer;

public class TestingAppend {

  public static void main(String[] args) throws IOException {
    File dir = new File("./layer");
    rmr(dir);
    dir.mkdirs();
    Random random = new Random(3);
    int blockSize = 4096;
    byte[] buf = new byte[blockSize];
    long total = 0;
    int layers = 10;
    for (int layer = 0; layer < layers; layer++) {
      File file = new File(dir, layer + ".layer");
      total += writeLayer(layer, file, random, blockSize, buf);
    }
    System.out.println(total);
    random = new Random(3);
    total = 0;
    for (int layer = 0; layer < layers; layer++) {
      File file = new File(dir, layer + ".layer");
      total += readLayer(layer, file, random, blockSize, buf);
    }
    System.out.println(total);
  }

  private static void rmr(File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        rmr(f);
      }
    }
    file.delete();
  }

  private static long readLayer(int layerId, File file, Random random, int blockSize, byte[] buf) throws IOException {
    long total = 0;
    byte[] readBuf = new byte[blockSize];
    try (Reader reader = new FileLayer.Reader(getInputReader(file))) {
      int blockId = random.nextInt(1000);
      for (int i = 0; i < 10000; i++) {
        random.nextBytes(buf);
        reader.readBlock(blockId, readBuf, 0, blockSize);
        if (!Arrays.equals(readBuf, buf)) {
          throw new IOException("data does not match!");
        }
        total += buf.length;
        blockId += (random.nextInt(1000) + 1);
      }
    }
    return total;
  }

  private static InputReader getInputReader(File file) throws IOException {
    RandomAccessFile rand = new RandomAccessFile(file, "r");
    long length = file.length();
    return new InputReader() {

      @Override
      public void close() throws IOException {
        rand.close();
      }

      @Override
      public long readLong(long position) throws IOException {
        rand.seek(position);
        return rand.readLong();
      }

      @Override
      public int readInt(long position) throws IOException {
        rand.seek(position);
        return rand.readInt();
      }

      @Override
      public void read(long position, byte[] buf, int offset, int length) throws IOException {
        rand.seek(position);
        rand.readFully(buf, offset, length);
      }

      @Override
      public int read(long position) throws IOException {
        rand.seek(position);
        return rand.read();
      }

      @Override
      public long length() {
        return length;
      }
    };
  }

  private static long writeLayer(long layerId, File file, Random random, int blockSize, byte[] buf) throws IOException {
    long total = 0;
    try (OutputStream outputStream = new FileOutputStream(file)) {
      try (Writer writer = new FileLayer.Writer(layerId, blockSize, outputStream)) {
        int blockId = random.nextInt(1000);
        for (int i = 0; i < 10000; i++) {
          random.nextBytes(buf);
          writer.append(blockId, buf);
          total += buf.length;
          blockId += (random.nextInt(1000) + 1);
        }
      }
    }
    return total;
  }

}
