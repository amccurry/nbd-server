package nbd.append.layer;

import java.io.Closeable;
import java.io.DataOutput;

public interface LayerOutput extends DataOutput, Closeable {

  long getPosition();

}
