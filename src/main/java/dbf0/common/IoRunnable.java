package dbf0.common;

import java.io.IOException;

public abstract class IoRunnable implements Runnable {

  public static IoRunnable wrap(IIoRunnable x) {
    return new Wrapper(x);
  }

  @Override
  public void run() {
    try {
      runIo();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract void runIo() throws IOException;

  public interface IIoRunnable {
    void runIo() throws IOException;
  }

  private static class Wrapper extends IoRunnable {

    private final IIoRunnable iIoRunnable;

    public Wrapper(IIoRunnable iIoRunnable) {
      this.iIoRunnable = iIoRunnable;
    }

    @Override
    protected void runIo() throws IOException {
      iIoRunnable.runIo();
    }
  }
}
