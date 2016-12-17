package nbd.append.layer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileLayerManager extends LayerManager {

  private static final Logger LOG = LoggerFactory.getLogger(FileLayerManager.class);

  private final File dir;
  private final Timer timer;

  public FileLayerManager(int blockSize, int maxCacheMemory, File dir) throws IOException {
    super(blockSize, maxCacheMemory);
    dir.mkdir();
    if (!dir.exists()) {
      throw new IOException("Path [" + dir + "] does not exist.");
    }
    this.dir = dir;
    this.timer = new Timer(dir.getAbsolutePath(), true);
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          releaseOldLayers();
        } catch (IOException e) {
          LOG.error("Unknown error during release of old layers.", e);
        }
      }
    }, TimeUnit.SECONDS.toMillis(10), TimeUnit.SECONDS.toMillis(10));
  }

  @Override
  public void close() throws IOException {
    timer.cancel();
    timer.purge();
    super.close();
  }

  @Override
  protected LayerOutput newOutput(long layerId) throws IOException {
    File file = new File(dir, Long.toString(layerId));
    file.getParentFile().mkdirs();
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
