package dbf0.document.types;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

public final class DMap extends DElement {

  private final ImmutableMap<DElement, DElement> entries;

  public DMap(ImmutableMap<DElement, DElement> entries) {
    this.entries = Preconditions.checkNotNull(entries);
  }

  public static DMap of(ImmutableMap<DElement, DElement> elements) {
    return new DMap(elements);
  }

  public static DMap of(Map<DElement, DElement> elements) {
    return new DMap(ImmutableMap.copyOf(elements));
  }

  public static DMap of() {
    return new DMap(ImmutableMap.of());
  }

  public static DMap of(DElement k, DElement v) {
    return new DMap(ImmutableMap.of(k, v));
  }

  public ImmutableMap<DElement, DElement> getEntries() {
    return entries;
  }

  @Override public @NotNull DElementType getType() {
    return DElementType.MAP;
  }

  @Override protected int compareToSameType(DElement o) {
    var that = (DMap) o;
    var commonKeys = Sets.intersection(entries.keySet(), that.entries.entrySet())
        .stream().sorted().iterator();
    while (commonKeys.hasNext()) {
      var key = commonKeys.next();
      var c = entries.get(key).compareTo(that.entries.get(key));
      if (c != 0) {
        return c;
      }
    }
    return Integer.compare(entries.size(), that.entries.size());
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DMap dArray = (DMap) o;
    return entries.equals(dArray.entries);
  }

  @Override public int hashCode() {
    return entries.hashCode();
  }

  @Override public String toString() {
    return "DMap{" + Joiner.on(", ").join(entries.entrySet().stream().sorted(Map.Entry.comparingByKey())
        .map(e -> e.getKey() + "=" + e.getValue()).iterator()) + "}";
  }
}
