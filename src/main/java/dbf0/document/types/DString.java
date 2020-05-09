package dbf0.document.types;

import com.google.common.base.Preconditions;
import org.jetbrains.annotations.NotNull;

public final class DString extends DElement {

  private final String value;

  public DString(String value) {
    this.value = Preconditions.checkNotNull(value);
  }

  public static DString of(String value) {
    return new DString(value);
  }

  public String getValue() {
    return value;
  }

  @Override public @NotNull DElementType getType() {
    return DElementType.STRING;
  }

  @Override protected int compareToSameType(DElement o) {
    return CharSequence.compare(value, ((DString) o).value);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DString dString = (DString) o;
    return value.equals(dString.value);
  }

  @Override public int hashCode() {
    return value.hashCode();
  }

  @Override public String toString() {
    return "DString{" + value + "}";
  }
}
