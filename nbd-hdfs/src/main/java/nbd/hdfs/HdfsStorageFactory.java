package nbd.hdfs;

import java.io.IOException;
import java.util.Properties;
import java.util.ServiceLoader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nbd.NBDStorage;
import nbd.NBDStorageFactory;
import nbd.append.AppendStorage;
import nbd.append.layer.LayerStorage;

public class HdfsStorageFactory extends NBDStorageFactory {

  private static Logger LOGGER = LoggerFactory.getLogger(HdfsStorageFactory.class);

  private static final String BLOCK_SIZE = "blockSize";
  private static final String MAX_CACHE_MEMORY = "maxCacheMemory";
  private static final String SIZE = "size";
  private static final String EXPORT_METADATA = "export.metadata";
  private static final String PATH = "path";
  private final Path root;
  private final Configuration configuration;

  public HdfsStorageFactory() {
    super("hdfs");
    String pathStr = getRequiredProperty(PATH);
    root = new Path(pathStr);
    configuration = new Configuration();
    ClassLoader classLoader = getClass().getClassLoader();
    System.out.println(classLoader);
    ServiceLoader<FileSystem> serviceLoader = ServiceLoader.load(FileSystem.class, classLoader);
    for (FileSystem fs : serviceLoader) {
      LOGGER.info("Loading filesystem type {} class {}", fs.getScheme(), fs.getClass());
      System.out.println("Loading filesystem type " + fs.getScheme() + " class " + fs.getClass());
      configuration.setClass("fs." + fs.getScheme() + ".impl", fs.getClass(), FileSystem.class);
    }
  }

  @Override
  public NBDStorage newStorage(String exportName) throws IOException {
    Path path = new Path(root, exportName);
    FileSystem fileSystem = path.getFileSystem(configuration);
    if (!fileSystem.exists(path)) {
      throw new IOException("Path " + path + " does not exist");
    }
    Path metaData = new Path(path, EXPORT_METADATA);
    if (!fileSystem.exists(metaData)) {
      throw new IOException("Path " + metaData + " does not exist");
    }
    Properties properties = new Properties();
    try (FSDataInputStream inputStream = fileSystem.open(metaData)) {
      properties.load(inputStream);
    }
    int blockSize = Integer.parseInt(required(properties, BLOCK_SIZE));
    long size = Long.parseLong(required(properties, SIZE));
    int maxCacheMemory = Integer.parseInt(required(properties, MAX_CACHE_MEMORY));

    LayerStorage layerStorage = new HdfsLayerStorage(path, configuration, blockSize, maxCacheMemory);
    return new AppendStorage(exportName, layerStorage, blockSize, size);
  }

  private String required(Properties properties, String key) throws IOException {
    String property = properties.getProperty(key);
    if (property == null) {
      throw new IOException("Required export property missing " + key);
    }
    return property;
  }

  @Override
  public void create(String exportName, int blockSize, long size, Properties optionalProperties) throws IOException {
    Path path = new Path(root, exportName);
    FileSystem fileSystem = path.getFileSystem(configuration);
    if (fileSystem.exists(path)) {
      throw new IOException("Path " + path + " already exists");
    }
    Path metaData = new Path(path, EXPORT_METADATA);
    if (fileSystem.exists(metaData)) {
      throw new IOException("Path " + metaData + " already exists");
    }

    Properties properties = new Properties();
    properties.setProperty(BLOCK_SIZE, Integer.toString(blockSize));
    properties.setProperty(SIZE, Long.toString(size));
    String maxCacheMemoryStr = required(optionalProperties, MAX_CACHE_MEMORY);
    int maxCacheMemory = Integer.parseInt(maxCacheMemoryStr);
    properties.setProperty(MAX_CACHE_MEMORY, Integer.toString(maxCacheMemory));

    try (FSDataOutputStream output = fileSystem.create(metaData)) {
      properties.store(output, null);
    }
  }

}
