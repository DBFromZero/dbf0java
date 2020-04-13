package dbf0.base;

import java.time.Duration;
import java.util.Collection;

public class SleepingClient extends BaseClient {

  private final Duration duration;

  public SleepingClient(Collection<? extends BaseConnector> connectors, Duration duration) {
    super(connectors);
    this.duration = duration;
  }

  @Override
  protected void waitUntilComplete() throws InterruptedException {
    Thread.sleep(duration.toMillis());
  }
}
