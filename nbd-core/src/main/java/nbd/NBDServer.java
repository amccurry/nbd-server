/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package nbd;

import static nbd.NBD.INIT_PASSWD;
import static nbd.NBD.NBD_FLAG_HAS_FLAGS;
import static nbd.NBD.NBD_OPT_EXPORT_NAME;
import static nbd.NBD.OPTS_MAGIC_BYTES;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.newsclub.net.unix.AFUNIXServerSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class NBDServer {

  private static Logger LOGGER = LoggerFactory.getLogger(NBDServer.class);

  private static final String NBD_UNIX_SOCKET_PATH = "NBD_UNIX_SOCKET_PATH";
  private static final String NBD_ENABLE_UNIX_SOCKET = "NBD_ENABLE_UNIX_SOCKET";
  private static final String NBD_TCP_PORT = "NBD_TCP_PORT";
  private static final String NBD_PLUGIN_SKIP_ERRORS = "NBD_PLUGIN_SKIP_ERRORS";
  private static final String NBD_PLUGIN_DIR = "NBD_PLUGIN_DIR";

  public static void main(String[] args) throws IOException {
    NBDConfig config = getNBDConfig();
    ExecutorService threadPool = Executors.newFixedThreadPool(10);
    ListeningExecutorService service = MoreExecutors.listeningDecorator(threadPool);
    PluginNBDStorageFactory storageFactory = new PluginNBDStorageFactory(config);
    try (ServerSocket ss = getServerSocket(config)) {
      LOGGER.info("Listening for client connections");
      while (true) {
        service.submit(new VolumeServerRunner(ss.accept(), storageFactory, service));
      }
    }
  }

  private static NBDConfig getNBDConfig() {

    File pluginDir = getFileProp(NBD_PLUGIN_DIR);
    boolean pluginSkipErrors = getBooleanProp(NBD_PLUGIN_SKIP_ERRORS, false);

    int tcpPort = getIntProp(NBD_TCP_PORT, 10809);

    boolean enableUnixSocket = getBooleanProp(NBD_ENABLE_UNIX_SOCKET, false);
    File unixSocket = getFileProp(NBD_UNIX_SOCKET_PATH);

    return NBDConfig.builder().enableUnixSocket(enableUnixSocket).pluginDir(pluginDir).unixSocket(unixSocket)
        .pluginSkipErrors(pluginSkipErrors).tcpPort(tcpPort).build();
  }

  private static ServerSocket getServerSocket(NBDConfig nbdConfig) throws IOException {
    if (nbdConfig.isEnableUnixSocket()) {
      File unixSocket = nbdConfig.getUnixSocket();
      unixSocket.mkdirs();
      if (unixSocket.exists()) {
        unixSocket.delete();
      }
      AFUNIXSocketAddress addr = new AFUNIXSocketAddress(unixSocket);
      return AFUNIXServerSocket.bindOn(addr);
    } else {
      return new ServerSocket(nbdConfig.getTcpPort());
    }
  }

  static class VolumeServerRunner implements Runnable {

    private final Socket socket;
    private final NBDStorageFactory storageFactory;
    private final Closer closer;
    private final ListeningExecutorService service;

    public VolumeServerRunner(Socket socket, NBDStorageFactory storageFactory, ListeningExecutorService service) {
      this.closer = Closer.create();
      this.socket = closer.register(socket);
      this.storageFactory = storageFactory;
      this.service = service;
    }

    @Override
    public void run() {
      try {
        InetSocketAddress remoteSocketAddress = (InetSocketAddress) socket.getRemoteSocketAddress();
        LOGGER.info("Client connected from: {}", remoteSocketAddress.getAddress().getHostAddress());
        try (DataInputStream in = new DataInputStream(socket.getInputStream())) {
          try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {
            String exportName = performHandShake(in, out);
            LOGGER.info("Connecting client to {}", exportName);
            NBDStorage storage = closer.register(storageFactory.newStorage(exportName));
            try (NBDVolumeServer nbdVolumeServer = new NBDVolumeServer(storage, in, out, service)) {
              LOGGER.info("Volume mounted");
              nbdVolumeServer.handleConnection();
            }
          }
        }
      } catch (Throwable t) {
        LOGGER.error("Failed to connect", t);
      } finally {
        try {
          closer.close();
        } catch (IOException e) {
          LOGGER.error(e.getMessage(), e);
        }
      }
    }

    private String performHandShake(DataInputStream in, DataOutputStream out) throws IOException {
      out.write(INIT_PASSWD);
      out.write(OPTS_MAGIC_BYTES);
      out.writeShort(NBD_FLAG_HAS_FLAGS);
      out.flush();

      int clientFlags = in.readInt();
      LOGGER.info("clientFlags {}", clientFlags);
      long magic = in.readLong();
      LOGGER.info("magic {}", magic);
      int opt = in.readInt();
      if (opt != NBD_OPT_EXPORT_NAME) {
        throw new RuntimeException("We support only EXPORT options");
      }
      int length = in.readInt();
      byte[] bytes = new byte[length];
      in.readFully(bytes);
      return new String(bytes, Charsets.UTF_8);
    }

  }

  private static int getIntProp(String name, int defaultValue) {
    String prop = getStringProp(name);
    if (prop == null) {
      return defaultValue;
    }
    return Integer.parseInt(prop);
  }

  private static String getStringProp(String name) {
    return System.getenv(name);
  }

  private static boolean getBooleanProp(String name, boolean defaultValue) {
    String prop = getStringProp(name);
    if (prop == null) {
      return defaultValue;
    }
    return Boolean.parseBoolean(prop);
  }

  private static File getFileProp(String name) {
    String prop = getStringProp(name);
    if (prop == null) {
      return null;
    }
    return new File(prop);
  }
}
