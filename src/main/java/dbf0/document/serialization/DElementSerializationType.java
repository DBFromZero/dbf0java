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
  DECIMAL(6),
  STRING(7),
  ARRAY(8),
  MAP(9);

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
