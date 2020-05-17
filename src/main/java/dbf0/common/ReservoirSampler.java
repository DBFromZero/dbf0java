package dbf0.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

// https://en.wikipedia.org/wiki/Reservoir_sampling
public class ReservoirSampler<T> {
  private final List<T> sample;
  private final int maxElements;
  private final Random random;

  private int seen = 0;

  public ReservoirSampler(Random random, int maxElements) {
    this.random = random;
    this.maxElements = maxElements;
    this.sample = new ArrayList<>(maxElements);
  }

  public boolean isEmpty() {
    return sample.isEmpty();
  }

  public void add(T x) {
    if (sample.size() < maxElements) {
      sample.add(x);
    } else {
      int i = random.nextInt(seen);
      if (i < maxElements) {
        sample.set(i, x);
      }
    }
    seen++;
  }

  public T sample() {
    return sample.get(random.nextInt(sample.size()));
  }

  public List<T> getSampled() {
    return Collections.unmodifiableList(sample);
  }
}
