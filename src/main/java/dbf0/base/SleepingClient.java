package dbf0.base;

import com.google.common.base.Preconditions;

import java.time.Duration;
import java.util.function.Function;

public class SleepingClient extends BaseClient {

  private final Duration duration;

  public SleepingClient(int nThreads, Function<String, ? extends BaseConnector> connectorFactory, Duration duration) {
    super(nThreads, connectorFactory);
    this.duration = Preconditions.checkNotNull(duration);
  }

  @Override
  protected void waitUntilComplete() throws InterruptedException {
    Thread.sleep(duration.toMillis());
  }
}
