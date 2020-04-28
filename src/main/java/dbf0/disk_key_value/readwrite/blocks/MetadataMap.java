package dbf0.disk_key_value.readwrite.blocks;

import java.io.IOException;
import java.util.Map;

public interface MetadataMap<K, V> {

  V put(K key, V value) throws IOException;

  int size();

  V get(K key);

  boolean containsKey(K key);

  V delete(K key) throws IOException;

  void clear() throws IOException;

  void putAll(Map<K, V> map) throws IOException;

  Map<K, V> unmodifiableMap();
}
