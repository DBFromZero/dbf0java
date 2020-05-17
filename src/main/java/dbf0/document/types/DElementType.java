package dbf0.document.types;

public enum DElementType {
  NULL(1),
  BOOL(2),
  INT(3),
  DECIMAL(4),
  STRING(5),
  ARRAY(6),
  MAP(7);

  private final int typeCode;

  DElementType(int typeCode) {
    this.typeCode = typeCode;
  }

  public int getTypeCode() {
    return typeCode;
  }
}
