package dbf0.test;

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
