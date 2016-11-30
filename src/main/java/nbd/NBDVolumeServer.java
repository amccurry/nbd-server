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

import static nbd.NBD.EMPTY_124;
import static nbd.NBD.NBD_FLAG_HAS_FLAGS;
import static nbd.NBD.NBD_FLAG_SEND_FLUSH;
import static nbd.NBD.NBD_OK_BYTES;
import static nbd.NBD.NBD_REPLY_MAGIC_BYTES;
import static nbd.NBD.NBD_REQUEST_MAGIC;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;
import com.google.common.primitives.UnsignedInteger;
import com.google.common.primitives.UnsignedLong;

import nbd.NBD.Command;

public class NBDVolumeServer implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(NBDVolumeServer.class);

  private final DataInputStream in;
  private final DataOutputStream out;
  private final String exportName;
  private final Storage storage;
  private final Closer closer;

  public NBDVolumeServer(Storage storage, DataInputStream in, DataOutputStream out) throws IOException {
    this.exportName = storage.getExportName();
    this.storage = storage;
    this.in = in;
    this.out = out;
    this.closer = Closer.create();
    closer.register(in);
    closer.register(out);
    LOGGER.info("Mounting {} of size {}", exportName, storage.size());
    storage.connect();
  }

  private void writeReplyHeaderAndFlush(long handle) throws IOException {
    synchronized (out) {
      out.write(NBD_REPLY_MAGIC_BYTES);
      out.write(NBD_OK_BYTES);
      out.writeLong(handle);
      out.flush();
    }
  }

  public void handleConnection() throws Exception {
    out.writeLong(storage.size());
    out.writeShort(NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH);
    out.write(EMPTY_124);
    out.flush();

    while (true) {
      int requestMagic = in.readInt();// MAGIC
      if (requestMagic != NBD_REQUEST_MAGIC) {
        throw new IllegalArgumentException("Invalid magic number for request: " + requestMagic);
      }
      Command requestType = Command.values()[in.readInt()];
      long handle = in.readLong();
      UnsignedLong offset = UnsignedLong.fromLongBits(in.readLong());
      UnsignedInteger requestLength = UnsignedInteger.fromIntBits(in.readInt());
      if (requestLength.longValue() > Integer.MAX_VALUE) {
        // We could ultimately support this but it isn't common by any means
        throw new IllegalArgumentException("Failed to read, length too long: " + requestLength);
      }
      switch (requestType) {
      case READ: {
        byte[] buffer = new byte[requestLength.intValue()];
        LOGGER.info("Reading {} from {}", buffer.length, offset);
        storage.read(buffer, offset.longValue(), () -> {
          synchronized (out) {
            out.write(NBD_REPLY_MAGIC_BYTES);
            out.write(NBD_OK_BYTES);
            out.writeLong(handle);
            out.write(buffer);
            out.flush();
          }
        });
        break;
      }
      case WRITE: {
        byte[] buffer = new byte[requestLength.intValue()];
        in.readFully(buffer);
        LOGGER.info("Writing {} from {}", buffer.length, offset);
        storage.write(buffer, offset.longValue(), () -> {
          writeReplyHeaderAndFlush(handle);
        });
        break;
      }
      case DISCONNECT:
        LOGGER.info("Disconnecting {}", exportName);
        storage.disconnect();
        return;
      case FLUSH:
        LOGGER.info("Flushing");
        long start = System.currentTimeMillis();
        storage.flush(() -> {
          writeReplyHeaderAndFlush(handle);
          LOGGER.info("Flush complete: " + (System.currentTimeMillis() - start) + "ms");
        });
        break;
      case TRIM:
        LOGGER.warn("Trim unimplemented");
        writeReplyHeaderAndFlush(handle);
        break;
      case CACHE:
        LOGGER.warn("Cache unimplemented");
        break;
      }
    }

  }

  @Override
  public void close() throws IOException {
    closer.close();
  }
}
