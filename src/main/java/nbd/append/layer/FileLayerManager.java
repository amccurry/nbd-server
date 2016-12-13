package nbd.append.layer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

public class FileLayerManager extends LayerManager {

  private final File dir;

  public FileLayerManager(int blockSize, int maxCacheMemory, File dir) throws IOException {
    super(blockSize, maxCacheMemory);
    dir.mkdir();
    if (!dir.exists()) {
      throw new IOException("Path [" + dir + "] does not exist.");
    }
    this.dir = dir;
  }

  @Override
  protected LayerOutput newOutput(long layerId) throws IOException {
    File file = new File(dir, Long.toString(layerId));
    file.getParentFile()
        .mkdirs();
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
    new File(dir, Long.toString(layerId)).delete();
  }
}
