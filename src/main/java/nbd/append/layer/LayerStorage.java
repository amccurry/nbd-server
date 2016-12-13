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
   * Compacts the storage.
   */
  void compact() throws IOException;

  /**
   * Flushes the blocks to stable storage.
   */
  void flush() throws IOException;

}
