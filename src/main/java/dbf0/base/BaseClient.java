package dbf0.base;

import dbf0.Dbf0Util;

import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class BaseClient extends Thread {

  private static final Logger LOGGER = Dbf0Util.getLogger(BaseClient.class);

  private final int nThreads;
  private final Function<String, ? extends BaseConnector> connectorFactory;

  protected BaseClient(int nThreads, Function<String, ? extends BaseConnector> connectorFactory) {
    super("client");
    this.nThreads = nThreads;
    this.connectorFactory = connectorFactory;
  }

  abstract protected void waitUntilComplete() throws InterruptedException;

  @Override
  public void run() {
    LOGGER.info("Starting client with " + nThreads + " threads");
    var connectors = IntStream.range(0, nThreads)
        .mapToObj(i -> connectorFactory.apply("thread-" + i))
        .collect(Collectors.toList());
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
