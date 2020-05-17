package dbf0.document.types;

import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;

public final class DDecimal extends DElement {

  private final BigDecimal value;

  public DDecimal(BigDecimal value) {
    this.value = value;
  }

  public static DDecimal of(BigDecimal value) {
    return new DDecimal(value);
  }

  public BigDecimal getValue() {
    return value;
  }

  @Override public @NotNull DElementType getType() {
    return DElementType.DECIMAL;
  }

  @Override protected int compareToSameType(DElement o) {
    return value.compareTo(((DDecimal) o).value);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DDecimal decimal = (DDecimal) o;
    return value.equals(decimal.value);
  }

  @Override public int hashCode() {
    return value.hashCode();
  }

  @Override public String toString() {
    return "DDecimal{" + value + "}";
  }
}
