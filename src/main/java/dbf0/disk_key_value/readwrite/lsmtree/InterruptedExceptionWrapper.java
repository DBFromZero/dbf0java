package dbf0.disk_key_value.readwrite.lsmtree;

public class InterruptedExceptionWrapper extends RuntimeException {

  public InterruptedExceptionWrapper() {
  }

  public InterruptedExceptionWrapper(String message) {
    super(message);
  }
}
