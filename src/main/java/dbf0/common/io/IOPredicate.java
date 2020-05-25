package dbf0.common.io;

import java.io.IOException;
import java.util.function.Predicate;

public interface IOPredicate<T> {

  boolean test(T t) throws IOException;

  default Wrapper<T> unchcecked() {
    return wrap(this);
  }

  static <T> Wrapper<T> wrap(IOPredicate<T> x) {
    return new Wrapper<>(x);
  }

  static <T> IOPredicate<T> of(Predicate<T> predicate) {
    return predicate::test;
  }

  class Wrapper<T> implements Predicate<T> {

    private final IOPredicate<T> delegate;

    private Wrapper(IOPredicate<T> delegate) {
      this.delegate = delegate;
    }

    @Override public boolean test(T t) {
      try {
        return delegate.test(t);
      } catch (IOException e) {
        throw new IOExceptionWrapper(e);
      }
    }
  }
}
