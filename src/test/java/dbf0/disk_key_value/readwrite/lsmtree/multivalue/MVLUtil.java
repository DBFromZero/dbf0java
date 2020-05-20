package dbf0.disk_key_value.readwrite.lsmtree.multivalue;

import dbf0.common.ByteArrayWrapper;
import dbf0.common.io.StringSerializer;
import dbf0.common.io.UnsignedIntSerializer;
import dbf0.disk_key_value.readwrite.lsmtree.LsmTreeConfiguration;

import java.util.Comparator;
import java.util.Random;
import java.util.stream.IntStream;

class MVLUtil {
  static final Comparator<String> KEY_COMPARATOR = String::compareTo;
  static final Comparator<Integer> VALUE_COMPARATOR = Integer::compareTo;
  static final String KEY_A = "A";
  static final String KEY_B = "B";
  static final String KEY_C = "C";
  static final ValueWrapper<Integer> PUT_1 = put(1);
  static final ValueWrapper<Integer> PUT_2 = put(2);
  static final ValueWrapper<Integer> DEL_1 = del(1);

  static final LsmTreeConfiguration<String, ValueWrapper<Integer>> CONFIGURATION =
      LsmTreeConfiguration.<String, ValueWrapper<Integer>>builder()
          .withKeySerialization(StringSerializer.serializationPair())
          .withValueSerialization(ValueWrapper.serializationPair(UnsignedIntSerializer.serializationPair()))
          .withKeyComparator(String::compareTo)
          .build();

  static ValueWrapper<Integer> put(int i) {
    return new ValueWrapper<>(false, i);
  }

  static ValueWrapper<Integer> del(int i) {
    return new ValueWrapper<>(true, i);
  }

  static PutAndDeletes<String, Integer> randomPutAndDeletes(Random random) {
    var result = new PutAndDeletes<String, Integer>(300);
    IntStream.range(0, 50 + random.nextInt(100)).forEach(ignored -> {
      var key = ByteArrayWrapper.random(random, 1).hexString();
      var value = random.nextInt(64);
      if (random.nextFloat() < 0.2) {
        result.delete(key, value);
      } else {
        result.put(key, value);
      }
    });
    return result;
  }
}
