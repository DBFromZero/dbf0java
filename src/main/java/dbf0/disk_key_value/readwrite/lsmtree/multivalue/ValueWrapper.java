package dbf0.disk_key_value.readwrite.lsmtree.multivalue;

import com.google.common.base.MoreObjects;
import dbf0.common.io.Deserializer;
import dbf0.common.io.EndOfStream;
import dbf0.common.io.SerializationPair;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;

public final class ValueWrapper<V> {

  private final boolean isDelete;
  private final V value;

  public ValueWrapper(boolean isDelete, V value) {
    this.isDelete = isDelete;
    this.value = value;
  }

  public boolean isDelete() {
    return isDelete;
  }

  public V getValue() {
    return value;
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ValueWrapper<?> that = (ValueWrapper<?>) o;

    if (isDelete != that.isDelete) return false;
    return value.equals(that.value);
  }

  @Override public int hashCode() {
    int result = (isDelete ? 1 : 0);
    result = 31 * result + value.hashCode();
    return result;
  }

  @Override public String toString() {
    return MoreObjects.toStringHelper("W")
        .add("isDelete", isDelete)
        .add("value", value)
        .toString();
  }

  static <V> Comparator<ValueWrapper<V>> comparator(Comparator<V> comparator) {
    return (o1, o2) -> comparator.compare(o1.value, o2.value);
  }

  public static final int VALUE = 32;
  public static final int DELETE = 97;

  public static <V> SerializationPair<ValueWrapper<V>> serializationPair(SerializationPair<V> serializationPair) {
    return new SerializationPair<>(
        (s, x) -> {
          s.write(x.isDelete ? DELETE : VALUE);
          serializationPair.getSerializer().serialize(s, x.value);
        },
        new Deserializer<>() {
          @NotNull @Override public ValueWrapper<V> deserialize(InputStream s) throws IOException {
            boolean isDelete;
            var type = s.read();
            if (type < 0) {
              throw new EndOfStream();
            }
            switch (type) {
              case VALUE:
                isDelete = false;
                break;
              case DELETE:
                isDelete = true;
                break;
              default:
                throw new RuntimeException("Bad delete type " + type);
            }
            return new ValueWrapper<>(isDelete, serializationPair.getDeserializer().deserialize(s));
          }

          @Override public void skipDeserialize(InputStream s) throws IOException {
            var type = s.read();
            if (type < 0) {
              throw new EndOfStream();
            }
            serializationPair.getDeserializer().skipDeserialize(s);
          }
        }
    );
  }
}
