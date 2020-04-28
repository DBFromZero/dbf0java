package dbf0.disk_key_value.readwrite.btree;

public enum Capacity {
  C2(2),
  C4(4),
  C8(8),
  C128(128);

  Capacity(int capacity) {
    this.capacity = capacity;
  }

  public final int capacity;
}
