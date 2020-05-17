package dbf0.common.io;


import java.io.IOException;

public interface IoRunnable {

  void run() throws IOException;

  default Wrapper unchcecked() {
    return wrap(this);
  }

  static Wrapper wrap(IoRunnable x) {
    return new Wrapper(x);
  }

  class Wrapper implements Runnable {

    private final IoRunnable delegate;

    private Wrapper(IoRunnable delegate) {
      this.delegate = delegate;
    }

    @Override public void run() {
      try {
        delegate.run();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
