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
package nbd.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.google.common.io.Closer;

import nbd.ExecCommand;
import nbd.Storage;

public class FileStorage extends Storage {

  private static final String RW = "rw";
  private final File file;
  private Closer closer;
  private RandomAccessFile raf;

  public FileStorage(File file) {
    super(file.getName());
    this.file = file;
  }

  @Override
  public void connect() throws IOException {
    closer = Closer.create();
    raf = closer.register(new RandomAccessFile(file, RW));
  }

  @Override
  public void disconnect() throws IOException {
    closer.close();
  }

  @Override
  public ExecCommand read(byte[] buffer, long offset) {
    return () -> {
      synchronized (raf) {
        raf.seek(offset);
        raf.read(buffer);
      }
    };
  }

  @Override
  public ExecCommand write(byte[] buffer, long offset) {
    return () -> {
      synchronized (raf) {
        raf.seek(offset);
        raf.write(buffer);
      }
    };
  }

  @Override
  public ExecCommand flush() {
    return () -> {
      synchronized (raf) {
        raf.getFD()
           .sync();
      }
    };
  }

  @Override
  public long size() {
    return file.length();
  }

}
