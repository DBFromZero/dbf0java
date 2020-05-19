package dbf0.common.io;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public interface IOIterator<E> extends Closeable {

  boolean hasNext() throws IOException;

  E next() throws IOException;

  default void skip() throws IOException {
    next();
  }

  @Override default void close() throws IOException {

  }

  default void forEachRemaining(IOConsumer<? super E> action) throws IOException {
    Preconditions.checkNotNull(action);
    while (hasNext())
      action.accept(next());
  }

  default Wrapper<E> unchcecked() {
    return wrap(this);
  }

  default <R> IOIterator<R> transform(IOFunction<E, R> function) {
    return new TransformedIOIterator<>(this, function);
  }

  static <E> Wrapper<E> wrap(IOIterator<E> x) {
    return new Wrapper<>(x);
  }

  class Wrapper<E> implements Iterator<E> {

    private final IOIterator<E> delegate;

    private Wrapper(IOIterator<E> delegate) {
      this.delegate = delegate;
    }

    @Override public boolean hasNext() {
      try {
        return delegate.hasNext();
      } catch (IOException e) {
        throw new IOExceptionWrapper(e);
      }
    }

    @Override public E next() {
      try {
        return delegate.next();
      } catch (IOException e) {
        throw new IOExceptionWrapper(e);
      }
    }
  }

  static <E> IOIterator<E> of(Iterator<E> iterator) {
    return new IOIterator<E>() {
      @Override public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override public E next() throws IOException {
        return iterator.next();
      }
    };
  }
}
