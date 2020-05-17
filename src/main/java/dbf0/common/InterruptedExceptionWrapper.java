package dbf0.common;

public class InterruptedExceptionWrapper extends RuntimeException {

  public InterruptedExceptionWrapper(InterruptedException e) {
    super(e);
  }

  public InterruptedExceptionWrapper(String message, InterruptedException e) {
    super(message, e);
  }
}
