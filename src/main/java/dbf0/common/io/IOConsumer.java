package dbf0.common.io;

import java.io.IOException;
import java.util.function.Consumer;

public interface IOConsumer<T> {

  void accept(T t) throws IOException;

  default Wrapper<T> unchcecked() {
    return wrap(this);
  }

  static <T> Wrapper<T> wrap(IOConsumer<T> x) {
    return new Wrapper<>(x);
  }

  class Wrapper<T> implements Consumer<T> {

    private final IOConsumer<T> delegate;

    private Wrapper(IOConsumer<T> delegate) {
      this.delegate = delegate;
    }

    @Override public void accept(T t) {
      try {
        delegate.accept(t);
      } catch (IOException e) {
        throw new IOExceptionWrapper(e);
      }
    }
  }
}
