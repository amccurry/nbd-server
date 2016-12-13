package nbd.append;

import static org.junit.Assert.assertArrayEquals;

import java.io.File;
import java.util.Random;

import org.junit.Test;

import nbd.NBDCommand;

public class FileAppendStorageTest {

  @Test
  public void test() throws Exception {

    Random randomSetup = new Random(1);
    long seed = randomSetup.nextLong();
    File dir = new File("./test");
    TestUtils.rmr(dir);
    dir.mkdirs();
    int blockSize = randomSetup.nextInt(1000) + 1;
    int maxCacheMemory = randomSetup.nextInt(1000000);
    int size = 1024 * 1024 * 1024;

    FileAppendStorage storage = FileAppendStorage.create("test", dir, blockSize, maxCacheMemory, size);

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
