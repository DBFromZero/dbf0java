package dbf0.document.types;

import org.jetbrains.annotations.NotNull;

public final class DInt extends DElement {

  private final long value;

  public DInt(long value) {
    this.value = value;
  }

  public static DInt of(long value) {
    return new DInt(value);
  }

  public long getValue() {
    return value;
  }

  @Override public @NotNull DElementType getType() {
    return DElementType.INT;
  }

  @Override protected int compareToSameType(DElement o) {
    return Long.compare(value, ((DInt) o).value);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DInt dInt = (DInt) o;
    return value == dInt.value;
  }

  @Override public int hashCode() {
    return (int) (value ^ (value >>> 32));
  }

  @Override public String toString() {
    return "DInt{" + value + "}";
  }
}
