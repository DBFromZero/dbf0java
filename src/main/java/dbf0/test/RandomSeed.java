package dbf0.test;

import java.util.Random;

public enum RandomSeed {
  CAFE(0xCAFE),
  DEADBEEF(0xDEADBEEF),
  BAD(0xBAD);

  private final long seed;

  RandomSeed(long seed) {
    this.seed = seed;
  }

  public Random random() {
    return new Random(seed);
  }
}
