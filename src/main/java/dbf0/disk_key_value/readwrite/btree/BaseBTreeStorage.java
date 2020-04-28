package dbf0.disk_key_value.readwrite.btree;

public abstract class BaseBTreeStorage<K extends Comparable<K>, V> implements BTreeStorage<K, V> {

  private final BTreeConfig config;
  private long nextId;

  protected BaseBTreeStorage(BTreeConfig config, long nextId) {
    this.config = config;
    this.nextId = nextId;
  }

  protected BaseBTreeStorage(BTreeConfig config) {
    this(config, 0);
  }

  @Override public BTreeConfig getConfig() {
    return config;
  }

  protected abstract void nodeCreated(Node<K, V> node);

  @Override public LeafNode<K, V> createLeaf() {
    var node = new LeafNode<>(nextId++, config.getLeafCapacity(), this);
    nodeCreated(node);
    return node;
  }

  @Override public ParentNode<K, V> createParent() {
    var node = new ParentNode<>(nextId++, config.getParentCapacity(), this);
    nodeCreated(node);
    return node;
  }
}
