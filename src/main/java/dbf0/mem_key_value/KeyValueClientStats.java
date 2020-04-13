package dbf0.mem_key_value;

import java.util.concurrent.atomic.AtomicInteger;

class KeyValueClientStats {

  final AtomicInteger set;
  final AtomicInteger get;
  final AtomicInteger found;
  final AtomicInteger missingKey;

  KeyValueClientStats() {
    set = new AtomicInteger(0);
    get = new AtomicInteger(0);
    found = new AtomicInteger(0);
    missingKey = new AtomicInteger(0);
  }

  String statsString() {
    return set.get() + " " + get.get() + " " + found.get() + " " + missingKey.get();
  }
}
