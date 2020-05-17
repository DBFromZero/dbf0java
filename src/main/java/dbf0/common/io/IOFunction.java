package dbf0.common.io;

import java.io.IOException;
import java.util.function.Function;

public interface IOFunction<T, R> {

  R apply(T t) throws IOException;

  default Wrapper<T, R> unchcecked() {
    return wrap(this);
  }

  static <T, R> Wrapper<T, R> wrap(IOFunction<T, R> x) {
    return new Wrapper<>(x);
  }

  class Wrapper<T, R> implements Function<T, R> {

    private final IOFunction<T, R> delegate;

    private Wrapper(IOFunction<T, R> delegate) {
      this.delegate = delegate;
    }

    @Override public R apply(T t) {
      try {
        return delegate.apply(t);
      } catch (IOException e) {
        throw new IOExceptionWrapper(e);
      }
    }
  }
}
