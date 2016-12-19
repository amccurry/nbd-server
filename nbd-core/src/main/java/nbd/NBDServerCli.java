package nbd;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class NBDServerCli {

  public static void main(String[] args) throws IOException, ParseException {
    Options options = new Options();
    options.addOption(require(new Option("p", "pluginDir", true, "The plugin directory.")));
    options.addOption(require(new Option("d", "driver", true, "The driver name.")));
    options.addOption(require(new Option("e", "export", true, "The export name.")));
    options.addOption(require(new Option("b", "blockSize", true, "The block size.")));
    options.addOption(require(new Option("s", "size", true, "The size of the export.")));
    Option option = new Option("o", "optProp", true, "Storage driver specify property.");
    option.setArgs(2);
    options.addOption(option);
    try {
      DefaultParser parser = new DefaultParser();
      CommandLine commandLine = parser.parse(options, args);
      File pluginDir = new File(commandLine.getOptionValue("p"));
      PluginNBDStorageFactory storageFactory = new PluginNBDStorageFactory(pluginDir, true);

      String driver = commandLine.getOptionValue("d");
      String export = commandLine.getOptionValue("e");
      String blockSizeStr = commandLine.getOptionValue("b");
      String sizeStr = commandLine.getOptionValue("s");

      String driverPlusExportName = driver + "/" + export;
      int blockSize = Integer.parseInt(blockSizeStr);
      long size = Long.parseLong(sizeStr);
      Properties optionalProperties = new Properties();
      for (Option o : commandLine.getOptions()) {
        if (o.getOpt().equals("o")) {
          String[] values = o.getValues();
          optionalProperties.setProperty(values[0], values[1]);
        }
      }
      System.out.println("Creating export\n export=" + driverPlusExportName + "\n blockSize=" + blockSize + "\n size="
          + size + "\n props=" + optionalProperties);
      storageFactory.create(driverPlusExportName, blockSize, size, optionalProperties);
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("nbd-server-cli", options);
      System.exit(1);
    }
  }

  private static Option require(Option option) {
    option.setRequired(true);
    return option;
  }

}
