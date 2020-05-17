package dbf0.document.types;

import com.google.common.base.Suppliers;
import dbf0.common.io.SerializationPair;
import dbf0.common.io.SizePrefixedDeserializer;
import dbf0.common.io.SizePrefixedSerializer;
import dbf0.document.serialization.DElementDeserializer;
import dbf0.document.serialization.DElementSerializer;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

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

  private static final Supplier<SerializationPair<DElement>> SERIALIZATION_PAIR_SUPPLIER = Suppliers.memoize(
      () -> new SerializationPair<>(DElementSerializer.defaultCharsetInstance(), DElementDeserializer.defaultCharsetInstance()));

  public static SerializationPair<DElement> serializationPair() {
    return SERIALIZATION_PAIR_SUPPLIER.get();
  }

  private static final Supplier<SerializationPair<DElement>> SIZE_PREFIXED_SERIALIZATION_PAIR_SUPPLIER = Suppliers.memoize(
      () -> new SerializationPair<>(
          new SizePrefixedSerializer<>(DElementSerializer.defaultCharsetInstance()),
          new SizePrefixedDeserializer<>(DElementDeserializer.defaultCharsetInstance(), false)));


  public static SerializationPair<DElement> sizePrefixedSerializationPair() {
    return SIZE_PREFIXED_SERIALIZATION_PAIR_SUPPLIER.get();
  }
}
