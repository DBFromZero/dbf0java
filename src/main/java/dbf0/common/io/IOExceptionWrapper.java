package dbf0.common.io;

import java.io.IOException;

public class IOExceptionWrapper extends RuntimeException {
  public IOExceptionWrapper(IOException cause) {
    super(cause);
  }

  public IOExceptionWrapper(String message, IOException cause) {
    super(message, cause);
  }
}
