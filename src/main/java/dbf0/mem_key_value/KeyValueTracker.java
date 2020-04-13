package dbf0.mem_key_value;

import dbf0.ByteArrayWrapper;

import java.util.HashSet;

interface KeyValueTracker {

  static KeyValueTracker noopTracker() {
    return new KeyValueTracker() {
      @Override
      public void trackSetKey(ByteArrayWrapper key) {
      }

      @Override
      public boolean expectKeySet(ByteArrayWrapper key) {
        return false;
      }
    };
  }

  static KeyValueTracker memoryTracker() {
    var set = new HashSet<ByteArrayWrapper>();
    return new KeyValueTracker() {
      @Override
      public void trackSetKey(ByteArrayWrapper key) {
        set.add(key);
      }

      @Override
      public boolean expectKeySet(ByteArrayWrapper key) {
        return set.contains(key);
      }
    };
  }

  void trackSetKey(ByteArrayWrapper key);

  boolean expectKeySet(ByteArrayWrapper key);
}
