package dbf0.common.io;


import java.io.IOException;

public interface IORunnable {

  void run() throws IOException;

  default Wrapper unchcecked() {
    return wrap(this);
  }

  static Wrapper wrap(IORunnable x) {
    return new Wrapper(x);
  }

  class Wrapper implements Runnable {

    private final IORunnable delegate;

    private Wrapper(IORunnable delegate) {
      this.delegate = delegate;
    }

    @Override public void run() {
      try {
        delegate.run();
      } catch (IOException e) {
        throw new IOExceptionWrapper(e);
      }
    }
  }
}
