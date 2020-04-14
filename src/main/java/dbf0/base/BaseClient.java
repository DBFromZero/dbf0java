package dbf0.base;

import dbf0.common.Dbf0Util;

import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class BaseClient extends Thread {

  private static final Logger LOGGER = Dbf0Util.getLogger(BaseClient.class);

  private final Collection<? extends BaseConnector> connectors;

  protected BaseClient(Collection<? extends BaseConnector> connectors) {
    super("client");
    this.connectors = connectors;
  }

  abstract protected void waitUntilComplete() throws InterruptedException;

  @Override
  public void run() {
    LOGGER.info("Starting client with " + connectors.size() + " connectors");
    connectors.forEach(BaseConnector::start);
    try {
      waitUntilComplete();
      connectors.forEach(Thread::interrupt);
      connectors.forEach(BaseConnector::closeSocket);
      for (var thread : connectors) {
        thread.join();
      }
    } catch (Exception e) {
      LOGGER.log(Level.WARNING, e, () -> "Error closing socket in " + this);
    }
  }
}
