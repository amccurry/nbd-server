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

import static nbd.NBD.*;
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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import nbd.NBD.Command;

public class NBDVolumeServer implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(NBDVolumeServer.class);

  private static final boolean DEBUG = false;

  private final DataInputStream in;
  private final DataOutputStream out;
  private final String exportName;
  private final NBDStorage storage;
  private final Closer closer;
  private final ListeningExecutorService service;

  public NBDVolumeServer(NBDStorage storage, DataInputStream in, DataOutputStream out, ListeningExecutorService service)
      throws IOException {
    this.service = service;
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
    out.writeShort(NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_TRIM);
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
      CommandHandler commandHandler = new CommandHandler(requestType, handle, offset, requestLength, service);
      switch (requestType) {
      case READ:
        performRead(handle, commandHandler);
        break;
      case WRITE:
        performWrite(handle, commandHandler);
        break;
      case DISCONNECT:
        performDisconnect();
        return;
      case FLUSH:
        performFlush(handle, commandHandler);
        break;
      case TRIM:
        performTrim(handle, commandHandler);
        break;
      case CACHE:
        LOGGER.info("Cache unimplemented");
        break;
      }
    }
  }

  private void performTrim(long handle, CommandHandler commandHandler) throws IOException {
    LOGGER.info("Trim {}", commandHandler);
    long start = System.currentTimeMillis();
    NBDCommand callable = storage.trim(commandHandler.requestLength.longValue(), commandHandler.offset.longValue());
    commandHandler.handleRequest(callable, () -> {
      try {
        writeReplyHeaderAndFlush(handle);
        LOGGER.info("Trim complete: " + (System.currentTimeMillis() - start) + "ms");
      } catch (Throwable t) {
        LOGGER.error("trim error", t);
      }
    });
  }

  private void performDisconnect() throws IOException {
    LOGGER.info("Disconnecting {}", exportName);
    storage.disconnect();
  }

  private void performFlush(long handle, CommandHandler commandHandler) {
    LOGGER.info("Flushing");
    long start = System.currentTimeMillis();
    NBDCommand callable = storage.flush();
    commandHandler.handleRequest(callable, () -> {
      try {
        writeReplyHeaderAndFlush(handle);
        LOGGER.info("Flush complete: " + (System.currentTimeMillis() - start) + "ms");
      } catch (Throwable t) {
        LOGGER.error("flushing", t);
      }
    });
  }

  private void performWrite(long handle, CommandHandler commandHandler) throws IOException {
    checkThatRequestIsNotTooLarge(commandHandler.requestLength);
    byte[] buffer = new byte[commandHandler.requestLength.intValue()];
    in.readFully(buffer);
    if (DEBUG) {
      LOGGER.info("Writing {} from {}", buffer.length, commandHandler.offset);
    }

    NBDCommand callable = storage.write(buffer, commandHandler.offset.longValue());
    commandHandler.handleRequest(callable, () -> {
      try {
        writeReplyHeaderAndFlush(handle);
      } catch (Throwable t) {
        LOGGER.error("writing " + buffer.length + " from " + commandHandler.offset + "", t);
      }
    });
  }

  private void performRead(long handle, CommandHandler commandHandler) {
    checkThatRequestIsNotTooLarge(commandHandler.requestLength);
    byte[] buffer = new byte[commandHandler.requestLength.intValue()];
    if (DEBUG) {
      LOGGER.info("Reading {} from {}", buffer.length, commandHandler.offset);
    }

    NBDCommand callable = storage.read(buffer, commandHandler.offset.longValue());
    commandHandler.handleRequest(callable, () -> {
      try {
        synchronized (out) {
          out.write(NBD_REPLY_MAGIC_BYTES);
          out.write(NBD_OK_BYTES);
          out.writeLong(handle);
          out.write(buffer);
          out.flush();
        }
      } catch (Throwable t) {
        LOGGER.error("reading " + buffer.length + " from " + commandHandler.offset + "", t);
      }
    });
  }

  private void checkThatRequestIsNotTooLarge(UnsignedInteger requestLength) {
    if (requestLength.longValue() > Integer.MAX_VALUE) {
      // We could ultimately support this but it isn't common by any means
      throw new IllegalArgumentException("Failed to read, length too long: " + requestLength);
    }
  }

  static class CommandHandler {
    private final Command requestType;
    private final long handle;
    private final UnsignedLong offset;
    private final UnsignedInteger requestLength;
    private final ListeningExecutorService service;

    CommandHandler(Command requestType, long handle, UnsignedLong offset, UnsignedInteger requestLength,
        ListeningExecutorService service) {
      this.requestType = requestType;
      this.handle = handle;
      this.offset = offset;
      this.requestLength = requestLength;
      this.service = service;
    }

    @Override
    public String toString() {
      return "CommandHandler [requestType=" + requestType + ", handle=" + handle + ", offset=" + offset
          + ", requestLength=" + requestLength + "]";
    }

    void handleRequest(NBDCommand callable, Runnable success) {
      ListenableFuture<Void> explosion = service.submit(() -> {
        callable.call();
        return null;
      });
      Futures.addCallback(explosion, new FutureCallback<Void>() {
        public void onSuccess(Void v) {
          success.run();
        }

        public void onFailure(Throwable t) {
          LOGGER.error("requestType [" + requestType + "] handle [" + handle + "] offset [" + offset
              + "] requestLength [" + requestLength + "]", t);
        }
      });
    }
  }

  @Override
  public void close() throws IOException {
    closer.close();
  }
}
