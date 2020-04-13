package dbf0.mem_key_value;

import dbf0.Dbf0Util;
import dbf0.base.BaseClient;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.logging.Level;

public class CreateHashMapRaceCondition {

  private static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", 9000);

  public static void main(String[] args) throws Exception {
    Dbf0Util.enableConsoleLogging(Level.FINE);

    var server = KeyValueServer.hashMapKeyValueServer(ADDRESS, 4);

    var smallKeySpaceChecker = new KeyValueConnector(ADDRESS, "checker", 0.2f,
        new RandomizedKeyValueSource(1, 4),
        KeyValueTracker.memoryTracker(),
        new KeyValueClientStats()
    );
    var largeKeySpaceWriter = new KeyValueConnector(ADDRESS, "writer", 1.0f,
        new RandomizedKeyValueSource(8, 4),
        KeyValueTracker.noopTracker(),
        new KeyValueClientStats()
    );
    var client = new Client(server, smallKeySpaceChecker, largeKeySpaceWriter);

    server.start();
    Thread.sleep(100);

    client.start();
    client.join();

    server.closeServerSocket();
    server.join();
  }

  private static class Client extends BaseClient {

    private final KeyValueServer server;
    private final KeyValueConnector smallKeySpaceChecker;
    private final KeyValueConnector largeKeySpaceWriter;

    public Client(KeyValueServer server, KeyValueConnector smallKeySpaceChecker, KeyValueConnector largeKeySpaceWriter) {
      super(Set.of(smallKeySpaceChecker, largeKeySpaceWriter));
      this.server = server;
      this.smallKeySpaceChecker = smallKeySpaceChecker;
      this.largeKeySpaceWriter = largeKeySpaceWriter;
    }

    @Override
    protected void waitUntilComplete() throws InterruptedException {
      while (true) {
        System.out.println("size: " + server.size());
        System.out.println(smallKeySpaceChecker.getStats().statsString());
        System.out.println(largeKeySpaceWriter.getStats().statsString());
        System.out.println();

        Thread.sleep(500);
      }
    }
  }
}
