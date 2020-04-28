package dbf0.disk_key_value.readwrite.blocks;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MemoryMetadataMap<K, V> implements MetadataMap<K, V> {

  private final Map<K, V> map = new HashMap<>();

  @Override public int size() {
    return map.size();
  }

  @Override public V put(K key, V value) {
    return map.put(key, value);
  }

  @Override public boolean containsKey(K key) {
    return map.containsKey(key);
  }

  @Override public V get(K key) {
    return map.get(key);
  }

  @Override public V delete(K key) {
    return map.remove(key);
  }

  @Override public void clear() {
    map.clear();
  }

  @Override public void putAll(Map<K, V> source) {
    map.putAll(source);
  }

  @Override public Map<K, V> unmodifiableMap() {
    return Collections.unmodifiableMap(map);
  }
}
