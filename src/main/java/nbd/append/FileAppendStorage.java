package nbd.append;

import java.io.File;
import java.io.IOException;

import nbd.append.layer.FileLayerManager;
import nbd.append.layer.LayerStorage;

public class FileAppendStorage extends AppendStorage {

  public static FileAppendStorage create(String exportName, File dir, int blockSize, int maxCacheMemory, long size)
      throws IOException {
    FileLayerManager layerManager = new FileLayerManager(blockSize, maxCacheMemory, new File(dir, exportName));
    layerManager.open();
    size = (size / blockSize) * blockSize;
    return new FileAppendStorage(exportName, layerManager, blockSize, size);
  }

  private long size;

  public FileAppendStorage(String exportName, LayerStorage layerStorage, int blockSize, long size) {
    super(exportName, layerStorage, blockSize);
    this.size = size;
  }

  @Override
  public void connect() throws IOException {

  }

  @Override
  public void disconnect() throws IOException {

  }

  @Override
  public long size() {
    return size;
  }

}
