package dbf0.test;

public enum KeySetSize {
  S1(1),
  S10(10),
  S100(100),
  S1000(1000);

  public final int size;

  KeySetSize(int size) {
    this.size = size;
  }
}
