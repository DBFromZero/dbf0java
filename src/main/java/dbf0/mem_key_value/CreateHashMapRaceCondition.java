package dbf0.mem_key_value;

import dbf0.Dbf0Util;
import dbf0.base.BaseClient;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class CreateHashMapRaceCondition {

  private static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", 9000);

  public static void main(String[] args) throws Exception {
    Dbf0Util.enableConsoleLogging(Level.INFO);

    var server = KeyValueServer.hashMapKeyValueServer(ADDRESS, 4);

    var smallKeySpaceChecker = new ConnectorWrapper("checker", 1, 4, 0.2f);
    var largeKeySpaceWriter = new ConnectorWrapper("writer", 8, 4, 1.0f);
    var client = new Client(smallKeySpaceChecker, largeKeySpaceWriter);

    server.start();
    Thread.sleep(100);

    client.start();
    client.join();

    server.closeServerSocket();
    server.join();
  }

  private static class ConnectorWrapper {

    private final KeyValueConnector connector;

    private final AtomicInteger setCount;
    private final AtomicInteger getCount;
    private final AtomicInteger findCount;
    private final AtomicInteger badValueCount;

    private ConnectorWrapper(String name, int keyLength, int valueLength, float setFraction) {
      setCount = new AtomicInteger(0);
      getCount = new AtomicInteger(0);
      findCount = new AtomicInteger(0);
      badValueCount = new AtomicInteger(0);

      connector = new KeyValueConnector(ADDRESS, name, keyLength, valueLength, setFraction,
          setCount, getCount, findCount, badValueCount);
    }

    private String results() {
      return connector.getName() +
          " " + setCount.get() + " " + getCount.get() + " " + findCount.get() + " " + badValueCount.get();
    }
  }

  private static class Client extends BaseClient {

    private final ConnectorWrapper smallKeySpaceChecker;
    private final ConnectorWrapper largeKeySpaceWriter;

    public Client(ConnectorWrapper smallKeySpaceChecker, ConnectorWrapper largeKeySpaceWriter) {
      super(Set.of(smallKeySpaceChecker.connector, largeKeySpaceWriter.connector));
      this.smallKeySpaceChecker = smallKeySpaceChecker;
      this.largeKeySpaceWriter = largeKeySpaceWriter;
    }

    @Override
    protected void waitUntilComplete() throws InterruptedException {
      while (true) {
        System.out.println(smallKeySpaceChecker.results());
        System.out.println(largeKeySpaceWriter.results());
        System.out.println();

        Thread.sleep(500);
      }
    }
  }
}
