package dbf0.disk_key_value.readwrite.log;

import dbf0.common.ByteArrayWrapper;
import org.jetbrains.annotations.NotNull;

public abstract class LogEntry {

  private final ByteArrayWrapper key;

  protected LogEntry(@NotNull ByteArrayWrapper key) {
    this.key = key;
  }

  @NotNull public ByteArrayWrapper getKey() {
    return key;
  }

  public abstract boolean isPut();

  public abstract boolean isDelete();

  @NotNull public abstract ByteArrayWrapper getValue();

  public static LogEntry put(@NotNull ByteArrayWrapper key, @NotNull ByteArrayWrapper value) {
    return new Put(key, value);
  }

  public static LogEntry delete(@NotNull ByteArrayWrapper key) {
    return new Delete(key);
  }

  private static class Put extends LogEntry {
    private final ByteArrayWrapper value;

    private Put(@NotNull ByteArrayWrapper key, @NotNull ByteArrayWrapper value) {
      super(key);
      this.value = value;
    }

    @Override public boolean isPut() {
      return true;
    }

    @Override public boolean isDelete() {
      return false;
    }

    @Override public @NotNull ByteArrayWrapper getValue() {
      return value;
    }
  }

  private static class Delete extends LogEntry {

    private Delete(@NotNull ByteArrayWrapper key) {
      super(key);
    }

    @Override public boolean isPut() {
      return false;
    }

    @Override public boolean isDelete() {
      return true;
    }

    @Override public @NotNull ByteArrayWrapper getValue() {
      throw new IllegalStateException("Delete log entries do not have a value");
    }
  }
}
