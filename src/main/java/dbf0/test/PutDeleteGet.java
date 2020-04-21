package dbf0.test;

public enum PutDeleteGet {
  PUT_HEAVY(0.9, 0.05),
  GET_HEAVY(0.1, 0.05),
  DELETE_HEAVY(0.45, 0.45),
  BALANCED(0.33, 0.33);

  public final double putRate;
  public final double deleteRate;
  public final double getRate;

  PutDeleteGet(double putRate, double deleteRate) {
    this.putRate = putRate;
    this.deleteRate = deleteRate;
    this.getRate = 1.0 - putRate - deleteRate;
  }
}
