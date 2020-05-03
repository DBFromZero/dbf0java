package dbf0.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

// https://en.wikipedia.org/wiki/Reservoir_sampling
public class ReservoirSampler<T> {
  private final List<T> keys = new ArrayList<>();
  private final int maxKeys = 10 * 1000;
  private final Random random;
  private int keysSeen = 0;

  public ReservoirSampler(Random random) {
    this.random = random;
  }

  public boolean isEmpty() {
    return keys.isEmpty();
  }

  public void add(T key) {
    if (keys.size() <= maxKeys) {
      keys.add(key);
    } else {
      int i = random.nextInt(keysSeen);
      if (i < keys.size()) {
        keys.set(i, key);
      }
    }
    keysSeen++;
  }

  public T sample() {
    return keys.get(random.nextInt(keys.size()));
  }
}
