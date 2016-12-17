package nbd.append.layer;

import java.io.Closeable;
import java.io.DataOutput;
import java.io.OutputStream;

public interface LayerOutput extends DataOutput, Closeable {

  long getPosition();

  public static LayerOutput toLayerOutput(OutputStream outputStream) {
    return new LayerOutputStream(outputStream);
  }

}
