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
package nbd.append.layer;

import java.io.Closeable;
import java.io.IOException;

public interface LayerOutput extends Closeable {

  long getPosition() throws IOException;

  void writeByte(byte b) throws IOException;

  default void write(byte[] buf) throws IOException {
    write(buf, 0, buf.length);
  }

  void write(byte[] buf, int offset, int length) throws IOException;

  default void writeInt(int i) throws IOException {
    writeByte((byte) (i >> 24));
    writeByte((byte) (i >> 16));
    writeByte((byte) (i >> 8));
    writeByte((byte) i);
  }

  default void writeLong(long i) throws IOException {
    writeInt((int) (i >> 32));
    writeInt((int) i);
  }
}
