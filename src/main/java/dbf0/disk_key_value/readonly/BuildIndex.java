package dbf0.disk_key_value.readonly;

import dbf0.common.ByteArrayWrapper;
import org.jetbrains.annotations.NotNull;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

import static dbf0.disk_key_value.readonly.WriteSortedKeyValueFiles.DIRECTORY;

public class BuildIndex {

  public static void main(String[] args) throws Exception {
    var stream = new PositionTrackingStream(DIRECTORY + "/merged");
    var iterator = new KeyFileIterator(stream);
    var storage = new NaiveDiskKeyValueStorage(DIRECTORY + "/index");
    storage.initialize();
    int i = 0;
    while (true) {
      var position = stream.position;
      if (!iterator.hasNext()) {
        break;
      }
      var key = iterator.next();
      if (i % 10000 == 0) {
        System.out.println("reading " + i);
      }
      if (i % 1000 == 0) {
        storage.store(key, ByteArrayWrapper.of(ByteBuffer.allocate(8).putLong(position).array()));
      }
      i++;
    }
    storage.close();
  }

  private static class PositionTrackingStream extends FileInputStream {

    private long position = 0;

    public PositionTrackingStream(String name) throws FileNotFoundException {
      super(name);
    }

    @Override
    public int read() throws IOException {
      var r = super.read();
      if (r != -1) {
        position += 1;
      }
      return r;
    }

    @Override
    public int read(@NotNull byte[] b) throws IOException {
      var r = super.read(b);
      if (r != -1) {
        position += r;
      }
      return r;
    }

    @Override
    public int read(@NotNull byte[] b, int off, int len) throws IOException {
      var r = super.read(b, off, len);
      if (r != -1) {
        position += r;
      }
      return r;
    }

    @Override
    public long skip(long n) throws IOException {
      var r = super.skip(n);
      position += r;
      return r;
    }
  }

}
