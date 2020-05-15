package dbf0.common.io;

import java.io.IOException;
import java.util.function.Consumer;

public interface IoConsumer<T> {

  void accept(T t) throws IOException;

  default Wrapper<T> unchcecked() {
    return wrap(this);
  }

  static <T> Wrapper<T> wrap(IoConsumer<T> x) {
    return new Wrapper<>(x);
  }

  class Wrapper<T> implements Consumer<T> {

    private final IoConsumer<T> delegate;

    private Wrapper(IoConsumer<T> delegate) {
      this.delegate = delegate;
    }

    @Override public void accept(T t) {
      try {
        delegate.accept(t);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
