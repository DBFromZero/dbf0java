package misc;

import java.util.HashMap;
import java.util.stream.IntStream;

//https://stackoverflow.com/questions/18542037/how-to-prove-that-hashmap-in-java-is-not-thread-safe
public class CreateHashMapRaceCondition {

  public static void main(String[] args) throws Exception {
    final var map = new HashMap<>();
    var targetKey = 0b1111_1111_1111_1111; // 65 535
    var targetValue = "v";
    map.put(targetKey, targetValue);

    new Thread(() -> IntStream.range(0, targetKey).forEach(key -> map.put(key, "someValue")))
        .start();

    while (true) {
      if (!targetValue.equals(map.get(targetKey))) {
        throw new RuntimeException("HashMap is not thread safe.");
      }
    }
  }
}
