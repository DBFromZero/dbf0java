package dbf0.mem_key_value;

import dbf0.ByteArrayWrapper;

import java.util.Random;

class RandomizedKeyValueSource implements KeyValueSource {

  private final Random random;
  private final ByteArrayWrapper key;
  private final ByteArrayWrapper value;

  RandomizedKeyValueSource(int keyLength, int valueLength) {
    this.random = new Random();
    this.key = new ByteArrayWrapper(new byte[keyLength]);
    this.value = new ByteArrayWrapper(new byte[valueLength]);
  }

  @Override
  public ByteArrayWrapper generateKey() {
    random.nextBytes(key.getArray());
    return key;
  }

  @Override
  public ByteArrayWrapper generateValueForKey(ByteArrayWrapper key) {
    // for each key, deterministically compute a companion value
    var k = key.getArray();
    var v = value.getArray();
    for (int i = 0; i < v.length; i++) {
      v[i] = (byte) ~(int) k[i % k.length];
    }
    return value;
  }
}
