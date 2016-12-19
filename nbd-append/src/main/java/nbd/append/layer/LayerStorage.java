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

public interface LayerStorage extends Closeable {

  void open() throws IOException;

  /**
   * Fully reads block into the buffer. Buffer length has to match the block
   * size of the layer storage.
   */
  void readBlock(int blockId, byte[] buffer) throws IOException;

  /**
   * Fully reads block into the buffer. Buffer can be larger than the block size
   * and the offset specifies where the buffer will begin writing into the
   * buffer.
   */
  void readBlock(int blockId, byte[] buffer, int offset) throws IOException;

  /**
   * Fully writes block into the storage. Buffer length has to match the block
   * size of the layer storage.
   */
  void writeBlock(int blockId, byte[] buffer) throws IOException;

  /**
   * Fully writes block into the storage. Buffer can be larger than the block
   * size and the offset specifies where the buffer will begin writing into the
   * buffer.
   */
  void writeBlock(int blockId, byte[] buffer, int offset) throws IOException;

  /**
   * Trims the blocks by writing zeros.
   * 
   * @param startingBlockId
   *          the starting block to trim.
   * @param count
   *          the number of blocks to trim.
   * @throws IOException
   */
  void trim(int startingBlockId, int count) throws IOException;

  /**
   * Releases layers that no longer have data that is visible to the block
   * device.
   */
  void releaseOldLayers() throws IOException;

  /**
   * Compacts all layer less than maxSize.
   */
  void compact(long maxSize) throws IOException;

  /**
   * Flushes the blocks to stable storage.
   */
  void flush() throws IOException;

}
