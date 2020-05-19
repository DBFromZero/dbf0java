package dbf0.disk_key_value.readwrite.lsmtree.multivalue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@VisibleForTesting
class PutAndDeletes<K, V> {

  private final HashMultimap<K, V> puts;
  private final HashMultimap<K, V> deletes;

  PutAndDeletes(int expectedSize) {
    expectedSize = Math.max(expectedSize, 32);
    puts = HashMultimap.create(expectedSize >> 2, 4);
    deletes = HashMultimap.create(expectedSize >> 4, 2);
  }

  int size() {
    return puts.size() + deletes.size();
  }

  void put(K key, V value) {
    puts.put(key, value);
    deletes.remove(key, value);
  }

  void delete(K key, V value) {
    deletes.put(key, value);
    puts.remove(key, value);
  }

  Collection<ValueWrapper<V>> getValues(K key) {
    var ps = puts.get(key);
    var ds = deletes.get(key);
    var pn = ps.size();
    var dn = ds.size();
    if (pn == 0 && dn == 0) {
      return List.of();
    }
    var list = new ArrayList<ValueWrapper<V>>(pn + dn);
    if (pn > 0) {
      for (V p : ps) {
        list.add(new ValueWrapper<>(false, p));
      }
    }
    if (dn > 0) {
      for (V d : ds) {
        list.add(new ValueWrapper<>(true, d));
      }
    }
    return list;
  }

  Multimap<K, ValueWrapper<V>> getCombined() {
    Multimap<K, ValueWrapper<V>> combined = HashMultimap.create(
        puts.asMap().size() + deletes.asMap().size(), 4);
    add(combined, puts, false);
    add(combined, deletes, true);
    return combined;
  }

  private void add(Multimap<K, ValueWrapper<V>> combined, HashMultimap<K, V> map, boolean isDelete) {
    map.forEach((k, v) -> combined.put(k, new ValueWrapper<>(isDelete, v)));
  }
}
