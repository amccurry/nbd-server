package nbd.append;

import java.io.IOException;

import nbd.NBDCommand;
import nbd.NBDStorage;
import nbd.append.layer.LayerStorage;

public class AppendStorage extends NBDStorage {

  protected final LayerStorage layerStorage;
  protected final int blockSize;
  protected final long size;

  public AppendStorage(String exportName, LayerStorage layerStorage, int blockSize, long size) {
    super(exportName);
    this.size = normalize(blockSize, size);
    this.layerStorage = layerStorage;
    this.blockSize = blockSize;
  }

  private static long normalize(int blockSize, long size) {
    return (size / blockSize) * blockSize;
  }

  @Override
  public void close() throws IOException {
    layerStorage.close();
  }

  @Override
  public NBDCommand trim(long length, long position) {
    return () -> trimDataFromLayerStorage(length, position, blockSize, layerStorage);
  }

  @Override
  public NBDCommand read(byte[] block, long position) {
    return () -> readDataFromLayerStorage(block, position, blockSize, layerStorage);
  }

  @Override
  public NBDCommand write(byte[] block, long position) {
    return () -> writeDataFromLayerStorage(block, position, blockSize, layerStorage);
  }

  @Override
  public NBDCommand flush() {
    return () -> layerStorage.flush();
  }

  interface Action {
    void action(int blockId, byte[] block) throws IOException;
  }

  public static void trimDataFromLayerStorage(long length, long position, int blockSize, LayerStorage layerStorage)
      throws IOException {
    byte[] alwaysEmpty = new byte[blockSize];
    byte[] buf = new byte[blockSize];
    while (length > 0) {
      int blockId = getBlockId(position, blockSize);
      int blockOffset = getBlockOffset(position, blockSize);

      if (blockOffset == 0) {
        // be greedy on how many blocks can be trimmed in one call
        int totalNumberOfBlocks = (int) (length / blockSize);
        layerStorage.trim(blockId, totalNumberOfBlocks);

        // this will account for only whole blocks worth of length
        long lengthOfValidData = (long) totalNumberOfBlocks * (long) blockSize;

        // Move pointers
        position += lengthOfValidData;
        length -= lengthOfValidData;
      } else {
        // hopefully not used, but if the trim somehow doesn't occur on block
        // breaks we have to zero out the other data
        //
        // read the whole block
        int lengthOfValidData = (int) Math.min(blockSize - blockOffset, length);

        readDataFromLayerStorage(buf, position, blockSize, layerStorage);
        System.arraycopy(alwaysEmpty, 0, buf, blockOffset, lengthOfValidData);
        layerStorage.writeBlock(blockId, buf);

        // Move pointers
        position += lengthOfValidData;
        length -= lengthOfValidData;
      }

    }
  }

  public static void writeDataFromLayerStorage(byte[] resultBlock, long position, int blockSize,
      LayerStorage layerStorage) throws IOException {
    byte[] buf = new byte[blockSize];
    int length = resultBlock.length;
    int resultBlockPos = 0;
    while (length > 0) {
      int blockId = getBlockId(position, blockSize);
      int blockOffset = getBlockOffset(position, blockSize);
      int lengthOfValidData = Math.min(blockSize - blockOffset, length);
      if (blockOffset == 0 && lengthOfValidData == blockSize) {
        System.arraycopy(resultBlock, resultBlockPos, buf, 0, lengthOfValidData);
        layerStorage.writeBlock(blockId, buf);
      } else {
        // read the whole block
        readDataFromLayerStorage(buf, position, blockSize, layerStorage);
        System.arraycopy(resultBlock, resultBlockPos, buf, blockOffset, lengthOfValidData);
        layerStorage.writeBlock(blockId, buf);
      }
      // Move pointers
      position += lengthOfValidData;
      resultBlockPos += lengthOfValidData;
      length -= lengthOfValidData;
    }
  }

  public static void readDataFromLayerStorage(byte[] resultBlock, long position, int blockSize,
      LayerStorage layerStorage) throws IOException {
    byte[] buf = new byte[blockSize];
    int length = resultBlock.length;
    int resultBlockPos = 0;
    while (length > 0) {
      int blockId = getBlockId(position, blockSize);
      int blockOffset = getBlockOffset(position, blockSize);
      layerStorage.readBlock(blockId, buf);
      int lengthOfValidData = Math.min(blockSize - blockOffset, length);
      System.arraycopy(buf, blockOffset, resultBlock, resultBlockPos, lengthOfValidData);

      // Move pointers
      position += lengthOfValidData;
      resultBlockPos += lengthOfValidData;
      length -= lengthOfValidData;
    }
  }

  @Override
  public void connect() throws IOException {

  }

  @Override
  public void disconnect() throws IOException {

  }

  @Override
  public long size() {
    return size;
  }

  public static int getBlockOffset(long position, int blockSize) {
    return (int) (position % blockSize);
  }

  public static int getBlockId(long position, int blockSize) {
    return (int) (position / blockSize);
  }
}
