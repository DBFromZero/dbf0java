package dbf0.disk_key_value.readwrite.lsmtree;

import dbf0.common.ByteArrayWrapper;
import dbf0.disk_key_value.readwrite.ReadWriteStorage;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;

public class LsmTree implements ReadWriteStorage<ByteArrayWrapper, ByteArrayWrapper>, Closeable {

  @Override public void close() throws IOException {
  }

  @Override public int size() throws IOException {
    return 0;
  }

  @Override public void put(@NotNull ByteArrayWrapper key, @NotNull ByteArrayWrapper value) throws IOException {

  }

  @Nullable @Override public ByteArrayWrapper get(@NotNull ByteArrayWrapper key) throws IOException {
    return null;
  }

  @Override public boolean delete(@NotNull ByteArrayWrapper key) throws IOException {
    return false;
  }
}
