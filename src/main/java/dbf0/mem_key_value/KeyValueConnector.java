package dbf0.mem_key_value;

import dbf0.base.BaseConnector;
import dbf0.common.Dbf0Util;
import dbf0.common.io.IOUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Random;
import java.util.logging.Logger;

class KeyValueConnector extends BaseConnector {

  // prefixes to identify different types of messages
  public static final byte SET = (byte) 's';
  public static final byte GET = (byte) 'g';
  public static final byte FOUND = (byte) 'f';
  public static final byte NOT_FOUND = (byte) 'n';

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

    s.getOutputStream().write(SET);
    IOUtil.writeBytes(s.getOutputStream(), key);
    IOUtil.writeBytes(s.getOutputStream(), value);

    stats.set.incrementAndGet();
    tracker.trackSetKey(key);
  }

  private void performGet(Socket s) throws IOException {
    var key = source.generateKey();
    LOGGER.finest(() -> "get key " + key);

    s.getOutputStream().write(GET);
    IOUtil.writeBytes(s.getOutputStream(), key);

    stats.get.incrementAndGet();

    int result = s.getInputStream().read();
    switch (result) {
      case -1:
        LOGGER.warning("unexpected end of stream");
        return;
      case FOUND:
        stats.found.incrementAndGet();
        var readValue = IOUtil.readBytes(s.getInputStream());
        LOGGER.finest(() -> String.format("found %s=%s", key, readValue));
        var expectedValue = source.generateValueForKey(key);
        if (!readValue.equals(expectedValue)) {
          LOGGER.warning(() -> String.format("incorrect value for %s: expected %s but given %s",
              key, expectedValue, readValue));
        }
        break;
      case NOT_FOUND:
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
