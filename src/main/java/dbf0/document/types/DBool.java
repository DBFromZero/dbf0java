package dbf0.document.types;

import org.jetbrains.annotations.NotNull;

public final class DBool extends DElement {

  private static final DBool TRUE = new DBool(true);
  private static final DBool FALSE = new DBool(false);

  public static DBool getTrue() {
    return TRUE;
  }

  public static DBool getFalse() {
    return FALSE;
  }

  public static DBool of(boolean b) {
    return b ? TRUE : FALSE;
  }

  private final boolean value;

  private DBool(boolean value) {
    this.value = value;
  }

  public boolean isTrue() {
    return value;
  }

  @Override public @NotNull DElementType getType() {
    return DElementType.BOOL;
  }

  @Override protected int compareToSameType(DElement o) {
    return this == o ? 0 : (value ? 1 : -1);
  }

  @Override public boolean equals(Object o) {
    return this == o;
  }

  @Override public int hashCode() {
    return Boolean.hashCode(value);
  }

  @Override public String toString() {
    return "DBool{" + value + "}";
  }
}
