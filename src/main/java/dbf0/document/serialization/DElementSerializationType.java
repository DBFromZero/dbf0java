package dbf0.document.serialization;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum DElementSerializationType {
  NULL(1),
  TRUE(2),
  FALSE(3),
  POS_INT(4),
  NEG_INT(5),
  STRING(6),
  ARRAY(7),
  MAP(8);

  private static Map<Integer, DElementSerializationType> CODE_TO_TYPE;

  private final int serializationCode;

  DElementSerializationType(int serializationCode) {
    this.serializationCode = serializationCode;
  }

  public static DElementSerializationType fromCode(int serializeCode) {
    if (CODE_TO_TYPE == null) {
      synchronized (DElementSerializationType.class) {
        if (CODE_TO_TYPE == null) {
          CODE_TO_TYPE = Arrays.stream(DElementSerializationType.values())
              .collect(Collectors.toUnmodifiableMap(DElementSerializationType::getSerializationCode, Function.identity()));
        }
      }
    }
    return CODE_TO_TYPE.get(serializeCode);
  }

  public int getSerializationCode() {
    return serializationCode;
  }
}
