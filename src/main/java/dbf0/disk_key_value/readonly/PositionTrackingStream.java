package dbf0.disk_key_value.readonly;

import org.jetbrains.annotations.NotNull;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

class PositionTrackingStream extends BufferedOutputStream {

  private long position = 0;

  PositionTrackingStream(String path) throws FileNotFoundException {
    super(new FileOutputStream(path), 0x8000);
  }

  long getPosition() {
    return position;
  }

  @Override public void write(int b) throws IOException {
    super.write(b);
    position += 1;
  }

  @Override public void write(@NotNull byte[] b, int off, int len) throws IOException {
    super.write(b, off, len);
    position += len;
  }

  // Note that write(byte[] b) is not override because that just calls the form with offset and length
}
