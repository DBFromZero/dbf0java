package dbf0.document.types;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.jetbrains.annotations.NotNull;

public final class DArray extends DElement {

  private final ImmutableList<DElement> elements;

  public DArray(ImmutableList<DElement> elements) {
    this.elements = Preconditions.checkNotNull(elements);
  }

  public static DArray of(ImmutableList<DElement> elements) {
    return new DArray(elements);
  }

  public static DArray of(Iterable<DElement> elements) {
    return new DArray(ImmutableList.copyOf(elements));
  }

  public static DArray of(DElement... elements) {
    return new DArray(ImmutableList.copyOf(elements));
  }

  public ImmutableList<DElement> getElements() {
    return elements;
  }

  @Override public @NotNull DElementType getType() {
    return DElementType.ARRAY;
  }

  @Override protected int compareToSameType(DElement o) {
    var that = (DArray) o;
    var compareLength = Math.min(elements.size(), that.elements.size());
    for (int i = 0; i < compareLength; i++) {
      var c = elements.get(i).compareTo(that.elements.get(i));
      if (c != 0) {
        return c;
      }
    }
    return Integer.compare(elements.size(), that.elements.size());
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DArray dArray = (DArray) o;
    return elements.equals(dArray.elements);
  }

  @Override public int hashCode() {
    return elements.hashCode();
  }

  @Override public String toString() {
    return "DArray{" + Joiner.on(", ").join(elements) + "}";
  }
}
