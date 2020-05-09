package dbf0.document.types;

public enum DElementType {
  NULL(1),
  BOOL(2),
  INT(3),
  STRING(4),
  ARRAY(5),
  MAP(6);

  private final int typeCode;

  DElementType(int typeCode) {
    this.typeCode = typeCode;
  }

  public int getTypeCode() {
    return typeCode;
  }
}
