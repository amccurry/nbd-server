package nbd;

import java.io.File;

import lombok.Builder;
import lombok.Data;
import lombok.Value;

@Data
@Value
@Builder(toBuilder = true)
public class NBDConfig {

  File pluginDir;

  boolean pluginSkipErrors;

  File unixSocket;

  boolean enableUnixSocket;

  int tcpPort;

}
