package dbf0.common.io;

import java.io.IOException;

public class EndOfStream extends IOException {

  public EndOfStream(String message) {
    super(message);
  }

  public EndOfStream() {
    this("Unexpected end of stream");
  }
}
