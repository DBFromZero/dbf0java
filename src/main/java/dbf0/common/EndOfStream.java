package dbf0.common;

import java.io.IOException;

public class EndOfStream extends IOException {

  public EndOfStream(String message) {
    super(message);
  }
}
