package dbf0.document.serialization;

import com.google.common.base.Strings;
import dbf0.common.io.IOUtil;
import dbf0.common.io.Serializer;
import dbf0.document.types.*;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

public class DElementSerializer implements Serializer<DElement> {

  static final int LOWER_4BITS_SET = 0xF;
  static final int FOURTH_BIT = 8;
  static final Charset DEFAULT_CHARSET = Charset.defaultCharset();

  private static final DElementSerializer DEFAULT_CHARSET_INSTANCE = new DElementSerializer(DEFAULT_CHARSET);

  @NotNull public static DElementSerializer defaultCharsetInstance() {
    return DEFAULT_CHARSET_INSTANCE;
  }

  @NotNull public static DElementSerializer forCharset(Charset charset) {
    return charset.equals(DEFAULT_CHARSET) ? defaultCharsetInstance() : new DElementSerializer(charset);
  }

  private final Charset charset;
  private final float estimatedBytesPerChar;

  private DElementSerializer(Charset charset) {
    this.charset = charset;
    var encoder = charset.newEncoder();
    this.estimatedBytesPerChar = Math.max(encoder.averageBytesPerChar(), 0.8F * encoder.maxBytesPerChar());
  }

  @Override public void serialize(OutputStream s, DElement x) throws IOException {
    serializeInternal(s, x);
  }


  @Override public int estimateSerializedSize(DElement x) {
    return sizeInternal(x);
  }

  private void serializeInternal(OutputStream s, DElement x) throws IOException {
    var type = x.getType();
    switch (type) {
      case NULL:
        s.write(DElementSerializationType.NULL.getSerializationCode());
        break;
      case BOOL:
        s.write((((DBool) x).isTrue() ? DElementSerializationType.TRUE : DElementSerializationType.FALSE)
            .getSerializationCode());
        break;
      case INT:
        serializeInt(s, (DInt) x);
        break;
      case DECIMAL:
        serializeDecimal(s, (DDecimal) x);
        break;
      case STRING:
        serializeString(s, (DString) x);
        break;
      case ARRAY:
        serializeArray(s, (DArray) x);
        break;
      case MAP:
        serializeMap(s, (DMap) x);
        break;
      default:
        throw new RuntimeException(Strings.lenientFormat("Unhandled type %s for %s", type, x));
    }
  }

  private void serializeInt(OutputStream s, DInt x) throws IOException {
    var value = x.getValue();
    if (value >= 0) {
      serializeUnsignedLong(s, DElementSerializationType.POS_INT, value);
    } else {
      serializeUnsignedLong(s, DElementSerializationType.NEG_INT, -value);
    }
  }

  private void serializeDecimal(OutputStream s, DDecimal x) throws IOException {
    var bytes = x.getValue().toString().getBytes(charset);
    serializeUnsignedLong(s, DElementSerializationType.DECIMAL, bytes.length);
    s.write(bytes);
  }

  private void serializeString(OutputStream s, DString x) throws IOException {
    var bytes = x.getValue().getBytes(charset);
    serializeUnsignedLong(s, DElementSerializationType.STRING, bytes.length);
    s.write(bytes);
  }

  private void serializeArray(OutputStream s, DArray x) throws IOException {
    var elements = x.getElements();
    var size = elements.size();
    serializeUnsignedLong(s, DElementSerializationType.ARRAY, size);
    for (int i = 0; i < size; i++) {
      serializeInternal(s, elements.get(i));
    }
  }

  private void serializeMap(OutputStream s, DMap x) throws IOException {
    var entries = x.getEntries();
    serializeUnsignedLong(s, DElementSerializationType.MAP, entries.size());
    for (var entry : entries.entrySet()) {
      serializeInternal(s, entry.getKey());
      serializeInternal(s, entry.getValue());
    }
  }

  private void serializeUnsignedLong(OutputStream s, DElementSerializationType type, long l) throws IOException {
    int lower4 = ((int) l) & LOWER_4BITS_SET;
    long left = l >>> 4;
    if (left == 0 && lower4 != LOWER_4BITS_SET) {
      s.write(type.getSerializationCode() | (lower4 << 4));
    } else {
      s.write(type.getSerializationCode() | (LOWER_4BITS_SET << 4));
      IOUtil.writeVariableLengthUnsignedLong(s, l);
    }
  }

  private int sizeInternal(DElement x) {
    var type = x.getType();
    switch (type) {
      case NULL:
      case BOOL:
        return 1;
      case INT:
        return sizeInt((DInt) x);
      case DECIMAL:
        return 10;
      case STRING:
        return sizeString((DString) x);
      case ARRAY:
        return sizeArray((DArray) x);
      case MAP:
        return sizeMap((DMap) x);
      default:
        throw new RuntimeException(Strings.lenientFormat("Unhandled type %s for %s", type, x));
    }
  }

  private int sizeInt(DInt x) {
    return sizeUnsignedLong(Math.abs(x.getValue()));
  }

  private int sizeString(DString x) {
    var l = x.getValue().length();
    return sizeUnsignedLong(l) + (int) Math.ceil(estimatedBytesPerChar * l);
  }

  private int sizeArray(DArray x) {
    var elements = x.getElements();
    var size = elements.size();
    var total = sizeUnsignedLong(size);
    for (int i = 0; i < size; i++) {
      total += sizeInternal(elements.get(i));
    }
    return total;
  }

  private int sizeMap(DMap x) {
    var entries = x.getEntries();
    var total = sizeUnsignedLong(entries.size());
    for (var entry : entries.entrySet()) {
      total += sizeInternal(entry.getKey());
      total += sizeInternal(entry.getValue());
    }
    return total;
  }

  private int sizeUnsignedLong(long l) {
    return IOUtil.sizeUnsignedLong(l, 4) + 1;
  }
}
