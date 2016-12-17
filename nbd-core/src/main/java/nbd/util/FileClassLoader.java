package nbd.util;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FileClassLoader extends URLClassLoader {

  private static final String JAR = ".jar";

  public FileClassLoader(URL... urls) {
    super(urls);
  }

  public FileClassLoader(File... files) throws IOException {
    super(toUrls(files));
  }

  public FileClassLoader(List<File> files) throws IOException {
    super(toUrls(files).toArray(new URL[] {}));
  }

  private static URL[] toUrls(File[] file) throws IOException {
    return toUrls(Arrays.asList(file)).toArray(new URL[] {});
  }

  private static List<URL> toUrls(List<File> files) throws IOException {
    List<URL> urls = new ArrayList<>();
    for (File f : files) {
      urls.addAll(toUrls(f));
    }
    return urls;
  }

  private static List<URL> toUrls(File file) throws IOException {
    List<URL> urls = new ArrayList<>();
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        urls.addAll(toUrls(f));
      }
    } else if (file.getName().endsWith(JAR)) {
      urls.add(file.toURI().toURL());
    }
    return urls;
  }

  public Class<?> loadClass(String name) throws ClassNotFoundException {
    return loadClass(name, false);
  }

  protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    Class<?> clazz = findLoadedClass(name);
    if (clazz == null) {
      try {
        clazz = findClass(name);
      } catch (ClassNotFoundException cnfe) {
        // do nothing
      }
    }
    if (clazz == null) {
      ClassLoader parent = getParent();
      if (parent != null) {
        clazz = parent.loadClass(name);
      } else {
        clazz = getSystemClassLoader().loadClass(name);
      }
    }
    if (resolve) {
      resolveClass(clazz);
    }
    return clazz;
  }

  public URL getResource(String name) {
    URL url = findResource(name);
    if (url == null) {
      url = getParent().getResource(name);
    }
    return url;
  }
}