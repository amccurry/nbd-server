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

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * This class is not to be used in server. Test use only.
 */
public class LayerInputRandomAccessFile implements LayerInput {

  private final RandomAccessFile rand;

  public LayerInputRandomAccessFile(RandomAccessFile rand) {
    this.rand = rand;
  }

  @Override
  public void seek(long position) throws IOException {
    rand.seek(position);
  }

  @Override
  public long getPosition() throws IOException {
    return rand.getFilePointer();
  }

  @Override
  public void close() throws IOException {
    rand.close();
  }

  @Override
  public long length() throws IOException {
    return rand.length();
  }

  @Override
  public byte readByte() throws IOException {
    return rand.readByte();
  }

  @Override
  public void read(byte[] buf, int offset, int length) throws IOException {
    rand.read(buf, offset, length);
  }

}
