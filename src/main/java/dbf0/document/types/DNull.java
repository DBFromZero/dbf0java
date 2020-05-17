package dbf0.document.types;

import org.jetbrains.annotations.NotNull;

public final class DNull extends DElement {

  private static final DNull INSTANCE = new DNull();

  public static DNull getInstance() {
    return INSTANCE;
  }

  private DNull() {
  }

  @NotNull @Override public DElementType getType() {
    return DElementType.NULL;
  }

  @Override protected int compareToSameType(DElement o) {
    return 0;
  }

  @Override public boolean equals(Object o) {
    return this == o;
  }

  @Override public int hashCode() {
    return 0;
  }

  @Override public String toString() {
    return "DNull";
  }
}
