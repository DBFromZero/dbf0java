package dbf0.document.types;

import dbf0.common.io.SerializationPair;
import dbf0.common.io.SizePrefixedDeserializer;
import dbf0.common.io.SizePrefixedSerializer;
import dbf0.document.serialization.DElementDeserializer;
import dbf0.document.serialization.DElementSerializer;
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

  public static SerializationPair<DElement> serializationPair() {
    return new SerializationPair<>(DElementSerializer.defaultCharsetInstance(), DElementDeserializer.defaultCharsetInstance());
  }

  public static SerializationPair<DElement> sizePrefixedSerializationPair() {
    return new SerializationPair<>(
        new SizePrefixedSerializer<>(DElementSerializer.defaultCharsetInstance()),
        new SizePrefixedDeserializer<>(DElementDeserializer.defaultCharsetInstance(), false));
  }
}
