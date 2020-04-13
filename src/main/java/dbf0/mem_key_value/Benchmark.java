package dbf0.mem_key_value;

import dbf0.Dbf0Util;
import dbf0.base.SleepingClient;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class Benchmark {

  private static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", 9000);

  public static void main(String[] args) throws Exception {
    Dbf0Util.enableConsoleLogging(Level.FINE);

    var server = KeyValueServer.concurrentHashMapKeyValueStore(ADDRESS, 4);

    final var setCount = new AtomicInteger(0);
    final var getCount = new AtomicInteger(0);
    final var findCount = new AtomicInteger(0);
    final var badValueCount = new AtomicInteger(0);
    var client = new SleepingClient(8, (name) -> new KeyValueConnector(
        ADDRESS, name,
        4, 64, 0.9f,
        setCount, getCount, findCount, badValueCount
    ), Duration.ofSeconds(10));

    server.start();
    Thread.sleep(100);

    client.start();
    client.join();

    System.out.println(setCount.get() + " " + getCount.get() + " " + findCount.get() + " " + badValueCount.get());

    server.closeServerSocket();
    server.join();
  }
}
