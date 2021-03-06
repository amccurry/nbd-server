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

import java.io.Closeable;
import java.io.IOException;

public abstract class NBDStorage implements Closeable {
  private final String exportName;

  public NBDStorage(String exportName) {
    this.exportName = exportName;
  }

  public final String getExportName() {
    return exportName;
  }

  public abstract void connect() throws IOException;

  public abstract void disconnect() throws IOException;

  public abstract NBDCommand read(byte[] buffer, long position);

  public abstract NBDCommand write(byte[] buffer, long position);

  public abstract NBDCommand trim(long length, long position);

  public abstract NBDCommand flush();

  public abstract long size();

}
