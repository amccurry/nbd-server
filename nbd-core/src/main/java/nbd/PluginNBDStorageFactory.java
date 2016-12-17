package nbd;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapMaker;

import nbd.util.FileClassLoader;

public class PluginNBDStorageFactory extends NBDStorageFactory {

  private static Logger LOGGER = LoggerFactory.getLogger(NBDServer.class);

  private final Map<String, NBDStorageFactory> factoryMap = new MapMaker().makeMap();

  public PluginNBDStorageFactory(File dir) throws IOException {
    super("plugin");
    loadPlugins(dir);
  }

  private void loadPlugins(File dir) throws IOException {
    LOGGER.info("Loading plugins from {}", dir.getAbsolutePath());
    if (!dir.isDirectory()) {
      throw new IOException("Plugins path [" + dir + "] is not directory.");
    }
    for (File f : dir.listFiles()) {
      loadPlugin(f);
    }
  }

  private void loadPlugin(File dir) throws IOException {
    if (!dir.isDirectory()) {
      LOGGER.info("Skipping path {} because it's not a directory.", dir);
      return;
    }
    LOGGER.info("Loading plugin from {}", dir.getAbsolutePath());
    FileClassLoader cl = new FileClassLoader(dir.listFiles());
    try {
      ServiceLoader<NBDStorageFactory> loader = ServiceLoader.load(NBDStorageFactory.class, cl);
      for (NBDStorageFactory factory : loader) {
        String driverName = factory.getDriverName();
        if (factoryMap.containsKey(driverName)) {
          throw new IOException("Factory driver name " + driverName + " has already been registered.");
        }
        LOGGER.info("Loading NBDStorageFactory {} {}", driverName, factory.getClass());
        factoryMap.put(driverName, factory);
      }
    } catch (ServiceConfigurationError e) {
      if (e.getCause() instanceof MissingPropertyException) {
        System.err.println(e.getCause().getMessage());
        System.exit(1);
      }
      throw e;
    }
  }

  @Override
  public NBDStorage newStorage(String driverPlusExportName) throws IOException {
    String driverName = getDriverName(driverPlusExportName);
    String exportName = getExportName(driverPlusExportName);
    NBDStorageFactory nbdStorageFactory = getStorageFactory(driverName);
    return nbdStorageFactory.newStorage(exportName);
  }

  @Override
  public void create(String driverPlusExportName, int blockSize, long size) throws IOException {
    String driverName = getDriverName(driverPlusExportName);
    String exportName = getExportName(driverPlusExportName);
    NBDStorageFactory nbdStorageFactory = getStorageFactory(driverName);
    nbdStorageFactory.create(exportName, blockSize, size);
  }

  private String getExportName(String s) throws IOException {
    int indexOf = s.indexOf('/');
    if (indexOf < 0) {
      throw new IOException("export name must contain driver name and export name. driver/export");
    }
    return s.substring(indexOf + 1);
  }

  private String getDriverName(String s) throws IOException {
    int indexOf = s.indexOf('/');
    if (indexOf < 0) {
      throw new IOException("export name must contain driver name and export name. driver/export");
    }
    return s.substring(0, indexOf);
  }

  private NBDStorageFactory getStorageFactory(String driverName) throws IOException {
    NBDStorageFactory nbdStorageFactory = factoryMap.get(driverName);
    if (nbdStorageFactory == null) {
      throw new IOException("driver name " + driverName + " not found");
    }
    return nbdStorageFactory;
  }

}
