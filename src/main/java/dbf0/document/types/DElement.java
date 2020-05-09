package dbf0.document.types;

import org.jetbrains.annotations.NotNull;

public abstract class DElement implements Comparable<DElement> {

  @NotNull public abstract DElementType getType();

  protected abstract int compareToSameType(DElement o);

  @Override public int compareTo(@NotNull DElement o) {
    if (this == o) {
      return 0;
    }
    var thisType = getType();
    var thatType = o.getType();
    if (thisType != thatType) {
      return Integer.compare(thatType.getTypeCode(), thatType.getTypeCode());
    }
    return compareToSameType(o);
  }
}
