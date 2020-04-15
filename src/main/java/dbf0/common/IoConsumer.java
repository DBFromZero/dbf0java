package dbf0.common;

import java.io.IOException;
import java.util.function.Consumer;

public abstract class IoConsumer<T> implements Consumer<T> {

  public static <T> IoConsumer<T> wrap(IIoConsumer<T> x) {
    return new Wrapper<>(x);
  }

  @Override
  public void accept(T t) {
    try {
      acceptIo(t);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract void acceptIo(T t) throws IOException;

  public interface IIoConsumer<T> {
    void accept(T t) throws IOException;
  }

  private static class Wrapper<T> extends IoConsumer<T> {

    private final IIoConsumer<T> IIoConsumer;

    public Wrapper(IIoConsumer<T> IIoConsumer) {
      this.IIoConsumer = IIoConsumer;
    }

    @Override
    protected void acceptIo(T t) throws IOException {
      IIoConsumer.accept(t);
    }
  }
}
