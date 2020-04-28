package dbf0.test;

public enum KnownKeyRate {
  LOW(0.1),
  MID(0.5),
  HIGH(0.9);

  public final double rate;

  KnownKeyRate(double rate) {
    this.rate = rate;
  }
}
