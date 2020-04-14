package dbf0.mem_key_value;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import dbf0.ByteArrayWrapper;
import dbf0.Dbf0Util;
import dbf0.base.SleepingClient;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.stream.IntStream;

public class Benchmark {

  private static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", 9000);

  private static final Map<String, Class<? extends Map>> MAP_CLASSES = ImmutableMap.of(
      "hashtable", Hashtable.class,
      "concurrent", ConcurrentHashMap.class
  );

  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 7);
    Dbf0Util.enableConsoleLogging(Level.INFO);

    var serverThreads = Integer.parseInt(args[0]);
    var clientThreads = Integer.parseInt(args[1]);
    var mapType = args[2];
    var keyLength = Integer.parseInt(args[3]);
    var valueLength = Integer.parseInt(args[4]);
    var setFrac = Float.parseFloat(args[5]);
    var duration = Duration.parse(args[6]);

    var map = ((Class<? extends Map<ByteArrayWrapper, ByteArrayWrapper>>) MAP_CLASSES.get(mapType))
        .getConstructor().newInstance();
    var server = new KeyValueServer(ADDRESS, serverThreads, map);

    var stats = new KeyValueClientStats();
    var client = new SleepingClient(IntStream.range(0, clientThreads).mapToObj(
        i -> new KeyValueConnector(ADDRESS, "conn-" + i, setFrac,
            new RandomKeyValueSource(keyLength, valueLength),
            KeyValueTracker.noopTracker(), stats))
        .collect(ImmutableList.toImmutableList()),
        duration
    );

    server.start();
    Thread.sleep(100);

    client.start();
    client.join();

    server.closeServerSocket();
    server.join();

    System.out.println(new Gson().toJson(stats));
  }
}
