package misc;

import dbf0.ByteArrayWrapper;

import java.util.HashMap;
import java.util.stream.IntStream;

//https://stackoverflow.com/questions/18542037/how-to-prove-that-hashmap-in-java-is-not-thread-safe
public class CreateHashMapRaceCondition {

  public static void main(String[] args) throws Exception {
    final var map = new HashMap<>();
    var targetKey = 0b1111_1111_1111_1111; // 65 535
    var targetValue = "v";
    var targetWrapper = makeKey(targetKey);
    map.put(targetWrapper, targetValue);

    new Thread(() -> IntStream.range(0, targetKey).forEach(key -> map.put(makeKey(key), "someValue")))
        .start();

    while (true) {
      if (!targetValue.equals(map.get(targetWrapper))) {
        throw new RuntimeException("HashMap is not thread safe.");
      }
    }
  }

  private static ByteArrayWrapper makeKey(int i) {
    return new ByteArrayWrapper(new byte[]{
        (byte) i,
        (byte) (i >> 8),
        (byte) (i >> 16),
        (byte) (i >> 24)
    });
  }
}
