package dbf0.test;

import java.util.Random;

public enum PutDeleteGet {
  PUT_HEAVY(0.9, 0.05),
  GET_HEAVY(0.1, 0.05),
  DELETE_HEAVY(0.45, 0.45),
  BALANCED(0.33, 0.33);

  public final double putRate;
  public final double deleteRate;
  public final double getRate;

  public Operation select(double r) {
    if (r < putRate) {
      return Operation.PUT;
    }
    r -= putRate;
    return r < deleteRate ? Operation.DELETE : Operation.GET;
  }

  public Operation select(Random random) {
    return select((random.nextDouble()));
  }

  PutDeleteGet(double putRate, double deleteRate) {
    this.putRate = putRate;
    this.deleteRate = deleteRate;
    this.getRate = 1.0 - putRate - deleteRate;
  }
}
