package nbd.append;

import nbd.ExecCommand;
import nbd.Storage;

public abstract class AppendStorage extends Storage {

  public AppendStorage(String exportName) {
    super(exportName);
  }

  @Override
  public ExecCommand read(byte[] buffer, long offset) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ExecCommand write(byte[] buffer, long offset) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ExecCommand flush() {
    // TODO Auto-generated method stub
    return null;
  }

}
