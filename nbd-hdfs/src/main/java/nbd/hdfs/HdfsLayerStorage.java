package nbd.hdfs;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import nbd.append.layer.LayerInput;
import nbd.append.layer.LayerManager;
import nbd.append.layer.LayerOutput;

public class HdfsLayerStorage extends LayerManager {

  private static final String LAYER_EXT = ".layer";
  private final Path root;
  private final FileSystem fileSystem;

  public HdfsLayerStorage(long size, Path root, Configuration configuration, int blockSize, int maxCacheMemory)
      throws IOException {
    super(size, blockSize, maxCacheMemory);
    this.root = root;
    fileSystem = root.getFileSystem(configuration);
  }

  @Override
  protected long[] getLayers() throws IOException {
    FileStatus[] listStatus = fileSystem.listStatus(root, (PathFilter) path -> path.getName()
                                                                                   .endsWith(LAYER_EXT));
    long[] layers = new long[listStatus.length];
    int index = 0;
    for (FileStatus fileStatus : listStatus) {
      String name = fileStatus.getPath()
                              .getName();
      layers[index++] = toLayerId(name);
    }
    return layers;
  }

  @Override
  protected long getNextLayerId() throws IOException {
    long[] layers = getLayers();
    Arrays.sort(layers);
    if (layers.length == 0) {
      return 1L;
    }
    return layers[layers.length - 1] + 1L;
  }

  @Override
  protected void removeLayer(long layerId) throws IOException {
    Path path = getPath(layerId);
    fileSystem.delete(path, false);
  }

  @Override
  protected LayerInput openLayer(long layerId) throws IOException {
    Path path = getPath(layerId);
    FileStatus fileStatus = fileSystem.getFileStatus(path);
    long length = fileStatus.getLen();
    FSDataInputStream inputStream = fileSystem.open(path);
    return new LayerInput() {

      @Override
      public void close() throws IOException {
        inputStream.close();
      }

      @Override
      public void seek(long position) throws IOException {
        inputStream.seek(position);
      }

      @Override
      public long getPosition() throws IOException {
        return inputStream.getPos();
      }

      @Override
      public byte readByte() throws IOException {
        return inputStream.readByte();
      }

      @Override
      public void read(byte[] buf, int offset, int length) throws IOException {
        inputStream.readFully(buf, offset, length);
      }

      @Override
      public long length() throws IOException {
        return length;
      }
    };
  }

  @Override
  protected LayerOutput newOutput(long layerId) throws IOException {
    Path path = getPath(layerId);
    FSDataOutputStream outputStream = fileSystem.create(path, false);
    return new LayerOutput() {

      @Override
      public void close() throws IOException {
        outputStream.close();
      }

      @Override
      public void writeByte(byte b) throws IOException {
        outputStream.write(b & 0xFF);
      }

      @Override
      public void write(byte[] buf, int offset, int length) throws IOException {
        outputStream.write(buf, offset, length);
      }

      @Override
      public long getPosition() throws IOException {
        return outputStream.getPos();
      }
    };
  }

  private Path getPath(long layerId) {
    return new Path(root, Long.toString(layerId) + LAYER_EXT);
  }

  private long toLayerId(String name) {
    int indexOf = name.indexOf(LAYER_EXT);
    if (indexOf < 0) {
      throw new RuntimeException("File " + name + " is not a layer file");
    }
    return Long.parseLong(name.substring(0, indexOf));
  }

}
