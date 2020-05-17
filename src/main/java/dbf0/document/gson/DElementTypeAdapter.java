package dbf0.document.gson;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import dbf0.document.types.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.regex.Pattern;

public class DElementTypeAdapter extends TypeAdapter<DElement> {

  public static final Pattern DECIMAL_COMPONENTS_PATTERN = Pattern.compile("[.eE]");

  private static final DElementTypeAdapter INSTANCE = new DElementTypeAdapter();

  public static DElementTypeAdapter getInstance() {
    return INSTANCE;
  }

  private DElementTypeAdapter() {
  }

  @Override public void write(JsonWriter out, DElement value) throws IOException {
    if (value == null) {
      out.nullValue();
      return;
    }
    var type = value.getType();
    switch (type) {
      case NULL:
        out.nullValue();
        break;
      case BOOL:
        out.value(((DBool) value).isTrue());
        break;
      case INT:
        out.value(((DInt) value).getValue());
        break;
      case DECIMAL:
        out.value(((DDecimal) value).getValue());
        break;
      case STRING:
        out.value(((DString) value).getValue());
        break;
      case ARRAY:
        out.beginArray();
        for (var element : ((DArray) value).getElements()) {
          write(out, element);
        }
        out.endArray();
        break;
      case MAP:
        out.beginObject();
        for (var entry : ((DMap) value).getEntries().entrySet()) {
          var key = entry.getKey();
          if (!(key instanceof DString)) {
            throw new IllegalArgumentException("Cannot convert document key " + key + " to a json object name");
          }
          out.name(((DString) key).getValue());
          write(out, entry.getValue());
        }
        out.endObject();
        break;
      default:
        throw new RuntimeException("Unhandled type " + type);
    }
  }

  @Override public DElement read(JsonReader in) throws IOException {
    switch (in.peek()) {
      case STRING:
        return new DString(in.nextString());
      case NUMBER:
        var number = in.nextString();
        return DECIMAL_COMPONENTS_PATTERN.matcher(number).find() ?
            DDecimal.of(new BigDecimal(number)) : DInt.of(Long.parseLong(number));
      case BOOLEAN:
        return DBool.of(in.nextBoolean());
      case NULL:
        in.nextNull();
        return DNull.getInstance();
      case BEGIN_ARRAY:
        ImmutableList.Builder<DElement> elements = ImmutableList.builder();
        in.beginArray();
        while (in.hasNext()) {
          elements.add(read(in));
        }
        in.endArray();
        return DArray.of(elements.build());
      case BEGIN_OBJECT:
        ImmutableMap.Builder<DElement, DElement> entries = ImmutableMap.builder();
        in.beginObject();
        while (in.hasNext()) {
          entries.put(new DString(in.nextName()), read(in));
        }
        in.endObject();
        return DMap.of(entries.build());
      case END_DOCUMENT:
      case NAME:
      case END_OBJECT:
      case END_ARRAY:
      default:
        throw new IllegalArgumentException();
    }
  }
}
