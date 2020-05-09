package dbf0.document.serialization;

public enum JElementSerializationType {
  NULL(1),
  TRUE(2),
  FALSE(3),
  POS_INT(4),
  NEG_INT(5),
  STRING(6),
  ARRAY(7),
  MAP(8);

  private final int serializationCode;

  JElementSerializationType(int serializationCode) {
    this.serializationCode = serializationCode;
  }
}
