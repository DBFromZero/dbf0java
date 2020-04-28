package dbf0.test;

import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public enum Count {
  N2(2),
  N4(4),
  N10(10),
  N100(100);

  Count(int count) {
    this.count = count;
  }

  public final int count;

  public IntStream range() {
    return IntStream.range(0, count);
  }

  public <T> Stream<T> times(Supplier<T> supplier) {
    return range().mapToObj(ignored -> supplier.get());
  }
}
