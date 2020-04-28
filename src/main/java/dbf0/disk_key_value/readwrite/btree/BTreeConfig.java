package dbf0.disk_key_value.readwrite.btree;

public class BTreeConfig {

  private final int leafCapacity;
  private final int parentCapacity;

  public BTreeConfig(int leafCapacity, int parentCapacity) {
    this.leafCapacity = leafCapacity;
    this.parentCapacity = parentCapacity;
  }

  public BTreeConfig(int capacity) {
    this(capacity, capacity);
  }

  public int getLeafCapacity() {
    return leafCapacity;
  }

  public int getParentCapacity() {
    return parentCapacity;
  }
}
