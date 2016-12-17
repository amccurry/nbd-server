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

import nbd.NBDStorage;
import nbd.NBDStorageFactory;

public class FileStorageFactory extends NBDStorageFactory {

  private static final String FILE = "file";
  private static final String DIR = "dir";
  private final File dir;

  public FileStorageFactory() {
    super(FILE);
    String dirStr = getRequiredProperty(DIR);
    this.dir = new File(dirStr);
  }

  @Override
  public NBDStorage newStorage(String exportName) {
    return new FileStorage(new File(dir, exportName));
  }

  @Override
  public void create(String exportName, int blockSize, long size) throws IOException {
    File file = new File(dir, exportName);
    if (file.exists()) {
      throw new IOException("export " + exportName + " already exists");
    }
    try (RandomAccessFile rand = new RandomAccessFile(file, "rw")) {
      rand.setLength(size);
    }
  }
}
