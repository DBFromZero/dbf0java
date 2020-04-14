package dbf0.mem_key_value;

import dbf0.Dbf0Util;
import dbf0.base.BaseConnector;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Random;
import java.util.logging.Logger;

class KeyValueConnector extends BaseConnector {

  private static final Logger LOGGER = Dbf0Util.getLogger(KeyValueConnector.class);

  private final float setFraction;
  private final KeyValueSource source;
  private final KeyValueTracker tracker;
  private final KeyValueClientStats stats;

  private final Random random;

  KeyValueConnector(InetSocketAddress connectAddress, String name,
                    float setFraction, KeyValueSource source,
                    KeyValueTracker tracker, KeyValueClientStats stats) {
    super(connectAddress, name);
    this.setFraction = setFraction;
    this.source = source;
    this.stats = stats;
    this.tracker = tracker;

    this.random = new Random();
  }

  public KeyValueClientStats getStats() {
    return stats;
  }

  @Override
  protected void processConnection(Socket s) throws IOException {
    if (random.nextFloat() < setFraction) {
      performSet(s);
    } else {
      performGet(s);
    }
  }

  private void performSet(Socket s) throws IOException {
    var key = source.generateKey();
    var value = source.generateValueForKey(key);
    LOGGER.finest(() -> "set key " + key + " to " + value);

    s.getOutputStream().write(PrefixIo.SET);
    PrefixIo.writePrefixLengthBytes(s.getOutputStream(), key);
    PrefixIo.writePrefixLengthBytes(s.getOutputStream(), value);

    stats.set.incrementAndGet();
    tracker.trackSetKey(key);
  }

  private void performGet(Socket s) throws IOException {
    var key = source.generateKey();
    LOGGER.finest(() -> "get key " + key);

    s.getOutputStream().write(PrefixIo.GET);
    PrefixIo.writePrefixLengthBytes(s.getOutputStream(), key);

    stats.get.incrementAndGet();

    int result = s.getInputStream().read();
    switch (result) {
      case -1:
        LOGGER.warning("unexpected end of stream");
        return;
      case PrefixIo.FOUND:
        stats.found.incrementAndGet();
        var readValue = PrefixIo.readPrefixLengthBytes(s.getInputStream());
        LOGGER.finest(() -> String.format("found %s=%s", key, readValue));
        var expectedValue = source.generateValueForKey(key);
        if (!readValue.equals(expectedValue)) {
          LOGGER.warning(() -> String.format("incorrect value for %s: expected %s but given %s",
              key, expectedValue, readValue));
        }
        break;
      case PrefixIo.NOT_FOUND:
        LOGGER.finest("not found");
        if (tracker.expectKeySet(key)) {
          stats.missingKey.incrementAndGet();
        }
        break;
      default:
        LOGGER.warning("bad result: " + result);
    }
  }
}
