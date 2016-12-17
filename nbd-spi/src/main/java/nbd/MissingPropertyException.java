package nbd;

public class MissingPropertyException extends RuntimeException {

  private static final long serialVersionUID = 2815285321214814730L;

  public MissingPropertyException(String message) {
    super(message);
  }

}
