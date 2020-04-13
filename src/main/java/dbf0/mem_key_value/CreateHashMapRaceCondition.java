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
    Dbf0Util.enableConsoleLogging(Level.FINE);

    var server = KeyValueServer.hashMapKeyValueServer(ADDRESS, 4);

    var smallKeySpaceChecker = new ConnectorWrapper("checker", 1, 4, 0.2f, true);
    var largeKeySpaceWriter = new ConnectorWrapper("writer", 8, 4, 1.0f, false);
    var client = new Client(server, smallKeySpaceChecker, largeKeySpaceWriter);

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
    private final AtomicInteger missingKeyCount;

    private ConnectorWrapper(String name, int keyLength, int valueLength, float setFraction, boolean trackKeys) {
      setCount = new AtomicInteger(0);
      getCount = new AtomicInteger(0);
      findCount = new AtomicInteger(0);
      missingKeyCount = new AtomicInteger(0);

      connector = new KeyValueConnector(ADDRESS, name, keyLength, valueLength, setFraction, trackKeys,
          setCount, getCount, findCount, missingKeyCount);
    }

    private String results() {
      return connector.getName() +
          " " + setCount.get() + " " + getCount.get() + " " + findCount.get() + " " + missingKeyCount.get();
    }
  }

  private static class Client extends BaseClient {

    private final KeyValueServer server;
    private final ConnectorWrapper smallKeySpaceChecker;
    private final ConnectorWrapper largeKeySpaceWriter;

    public Client(KeyValueServer server, ConnectorWrapper smallKeySpaceChecker, ConnectorWrapper largeKeySpaceWriter) {
      super(Set.of(smallKeySpaceChecker.connector, largeKeySpaceWriter.connector));
      this.server = server;
      this.smallKeySpaceChecker = smallKeySpaceChecker;
      this.largeKeySpaceWriter = largeKeySpaceWriter;
    }

    @Override
    protected void waitUntilComplete() throws InterruptedException {
      while (true) {
        System.out.println("size: " + server.size());
        System.out.println(smallKeySpaceChecker.results());
        System.out.println(largeKeySpaceWriter.results());
        System.out.println();

        Thread.sleep(500);
      }
    }
  }
}
