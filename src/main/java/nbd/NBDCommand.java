package nbd;

public interface NBDCommand {
  void call() throws Exception;
}
