package dbf0.common.io;

import java.io.IOException;
import java.util.function.Supplier;

public interface IOSupplier<T> {
  T get() throws IOException;

  default Wrapper<T> unchcecked() {
    return wrap(this);
  }

  static <T> Wrapper<T> wrap(IOSupplier<T> x) {
    return new Wrapper<>(x);
  }

  class Wrapper<T> implements Supplier<T> {

    private final IOSupplier<T> delegate;

    private Wrapper(IOSupplier<T> delegate) {
      this.delegate = delegate;
    }

    @Override public T get() {
      try {
        return delegate.get();
      } catch (IOException e) {
        throw new IOExceptionWrapper(e);
      }
    }
  }
}
