package dbf0.common;

import java.io.IOException;
import java.util.function.Function;

public abstract class IoFunction<T, R> implements Function<T, R> {

  public static <T, R> IoFunction<T, R> wrap(IIoFunction<T, R> x) {
    return new Wrapper<>(x);
  }

  @Override
  public R apply(T t) {
    try {
      return applyIo(t);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract R applyIo(T t) throws IOException;

  public interface IIoFunction<T, R> {
    R applyIo(T t) throws IOException;
  }

  private static class Wrapper<T, R> extends IoFunction<T, R> {

    private final IIoFunction<T, R> iIoFunction;

    public Wrapper(IIoFunction<T, R> iIoFunction) {
      this.iIoFunction = iIoFunction;
    }

    @Override
    protected R applyIo(T t) throws IOException {
      return iIoFunction.applyIo(t);
    }
  }
}
