package dbf0.mem_key_value;

import java.util.concurrent.atomic.AtomicInteger;

class KeyValueClientStats {

  final AtomicInteger set = new AtomicInteger(0);
  final AtomicInteger get = new AtomicInteger(0);
  final AtomicInteger found = new AtomicInteger(0);
  final AtomicInteger missingKey = new AtomicInteger(0);

  String statsString() {
    return set.get() + " " + get.get() + " " + found.get() + " " + missingKey.get();
  }
}
