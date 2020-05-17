package dbf0.common.io;

import com.google.common.base.Preconditions;
import org.jetbrains.annotations.NotNull;

public class SerializationPair<T> {

  private final Serializer<T> serializer;
  private final Deserializer<T> deserializer;

  public SerializationPair(@NotNull Serializer<T> serializer, @NotNull Deserializer<T> deserializer) {
    this.serializer = Preconditions.checkNotNull(serializer);
    this.deserializer = Preconditions.checkNotNull(deserializer);
  }

  @NotNull public Serializer<T> getSerializer() {
    return serializer;
  }

  @NotNull public Deserializer<T> getDeserializer() {
    return deserializer;
  }

}
