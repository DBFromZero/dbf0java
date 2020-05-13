package dbf0.document.serialization;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import dbf0.common.Dbf0Util;
import dbf0.common.EndOfStream;
import dbf0.common.io.Deserializer;
import dbf0.common.io.IOUtil;
import dbf0.document.types.*;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.Charset;

import static dbf0.document.serialization.DElementSerializer.DEFAULT_CHARSET;
import static dbf0.document.serialization.DElementSerializer.LOWER_4BITS_SET;

public class DElementDeserializer implements Deserializer<DElement> {

  private static final DElementDeserializer DEFAULT_CHARSET_INSTANCE = new DElementDeserializer(DEFAULT_CHARSET);
  private static final int MAX_INTERNED_STRINGS = 5000;

  @NotNull public static DElementDeserializer defaultCharsetInstance() {
    return DEFAULT_CHARSET_INSTANCE;
  }

  @NotNull public static DElementDeserializer forCharset(Charset charset) {
    Preconditions.checkNotNull(charset);
    return charset.equals(DEFAULT_CHARSET) ? defaultCharsetInstance() : new DElementDeserializer(charset);
  }

  private final Charset charset;

  private DElementDeserializer(Charset charset) {
    this.charset = charset;
  }

  @NotNull @Override public DElement deserialize(InputStream s) throws IOException {
    return deserializeInternal(s);
  }

  @Override public void skipDeserialize(InputStream s) throws IOException {
    skipInternal(s);
  }

  private DElement deserializeInternal(InputStream s) throws IOException {
    int codeAndExtra = s.read();
    if (codeAndExtra < 0) {
      throw new EndOfStream();
    }
    int code = codeAndExtra & LOWER_4BITS_SET;
    var type = DElementSerializationType.fromCode(code);
    if (type == null) {
      throw new RuntimeException("Unknown serialization code " + code);
    }
    switch (type) {
      case NULL:
        return DNull.getInstance();
      case TRUE:
        return DBool.getTrue();
      case FALSE:
        return DBool.getFalse();
      case POS_INT:
        return DInt.of(deserializeUnsignedLong(s, codeAndExtra));
      case NEG_INT:
        return DInt.of(-deserializeUnsignedLong(s, codeAndExtra));
      case DECIMAL:
        return DDecimal.of(new BigDecimal(deserializeString(s, codeAndExtra)));
      case STRING:
        return DString.of(deserializeString(s, codeAndExtra));
      case ARRAY:
        return deserializeArray(s, codeAndExtra);
      case MAP:
        return deserializeMap(s, codeAndExtra);
      default:
        throw new RuntimeException("Unhandled type " + type);
    }
  }

  private String deserializeString(InputStream s, int codeAndExtra) throws IOException {
    int length = deserializeUnsignedInt(s, codeAndExtra);
    var bytes = new byte[length];
    IOUtil.readArrayFully(s, bytes);
    return new String(bytes, charset);
  }

  private DArray deserializeArray(InputStream s, int codeAndExtra) throws IOException {
    int length = deserializeUnsignedInt(s, codeAndExtra);
    var elements = new DElement[length];
    for (int i = 0; i < length; i++) {
      elements[i] = deserializeInternal(s);
    }
    return DArray.of(elements);
  }

  private DMap deserializeMap(InputStream s, int codeAndExtra) throws IOException {
    int length = deserializeUnsignedInt(s, codeAndExtra);
    var builder = ImmutableMap.<DElement, DElement>builderWithExpectedSize(length);
    while (length-- > 0) {
      var key = deserializeInternal(s);
      var value = deserializeInternal(s);
      builder.put(key, value);
    }
    return DMap.of(builder.build());
  }

  private long deserializeUnsignedLong(InputStream s, int codeAndExtra) throws IOException {
    long value = codeAndExtra >>> 4;
    if (value != LOWER_4BITS_SET) {
      return value;
    }
    return IOUtil.readVariableLengthUnsignedLong(s);
  }

  private int deserializeUnsignedInt(InputStream s, int codeAndExtra) throws IOException {
    return Dbf0Util.safeLongToInt(deserializeUnsignedLong(s, codeAndExtra));
  }

  private void skipInternal(InputStream s) throws IOException {
    int codeAndExtra = s.read();
    if (codeAndExtra < 0) {
      throw new EndOfStream();
    }
    int code = codeAndExtra & LOWER_4BITS_SET;
    var type = DElementSerializationType.fromCode(code);
    if (type == null) {
      throw new RuntimeException("Unknown serialization code " + code);
    }
    switch (type) {
      case NULL:
      case TRUE:
      case FALSE:
        break;
      case POS_INT:
      case NEG_INT:
        deserializeUnsignedLong(s, codeAndExtra);
        break;
      case DECIMAL:
      case STRING:
        IOUtil.skip(s, deserializeUnsignedLong(s, codeAndExtra));
        break;
      case ARRAY:
        skipN(s, deserializeUnsignedInt(s, codeAndExtra));
        break;
      case MAP:
        skipN(s, 2 * deserializeUnsignedInt(s, codeAndExtra));
        break;
      default:
        throw new RuntimeException("Unhandled type " + type);
    }
  }

  private void skipN(InputStream s, int n) throws IOException {
    while (n-- > 0) {
      skipInternal(s);
    }
  }
}
