package nbd.append;

import java.io.File;
import java.util.Random;

public class TestUtils {
  public static void rmr(File file) {
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

  public static long getSeed() {
    return new Random().nextLong();
  }

  public static byte[] nextBytes(Random random, byte[] buf) {
    random.nextBytes(buf);
    return buf;
  }
}
