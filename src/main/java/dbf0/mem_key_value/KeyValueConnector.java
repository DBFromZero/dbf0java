package dbf0.mem_key_value;

import dbf0.ByteArrayWrapper;
import dbf0.Dbf0Util;
import dbf0.base.BaseConnector;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

class KeyValueConnector extends BaseConnector {

  private static final Logger LOGGER = Dbf0Util.getLogger(KeyValueConnector.class);
  private final float setFraction;

  private final AtomicInteger setCount;
  private final AtomicInteger getCount;
  private final AtomicInteger findCount;
  private final AtomicInteger missingKeyCount;

  private final Random random;
  private final ByteArrayWrapper key;
  private final ByteArrayWrapper value;
  private final Set<ByteArrayWrapper> setKeys;

  public KeyValueConnector(InetSocketAddress connectAddress, String name, int keyLength, int valueLength,
                           float setFraction, boolean trackKeys,
                           AtomicInteger setCount, AtomicInteger getCount, AtomicInteger findCount,
                           AtomicInteger missingKeyCount) {
    super(connectAddress, name);
    this.setFraction = setFraction;
    this.getCount = getCount;
    this.findCount = findCount;
    this.setCount = setCount;
    this.missingKeyCount = missingKeyCount;

    this.random = new Random();
    this.key = new ByteArrayWrapper(new byte[keyLength]);
    this.value = new ByteArrayWrapper(new byte[valueLength]);
    this.setKeys = trackKeys ? new HashSet<>() : null;
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
    randomizeKeyAndComputeValue();
    LOGGER.finest(() -> "set key " + key + " to " + value);

    s.getOutputStream().write(PrefixIo.SET);
    PrefixIo.writePrefixLengthBytes(s.getOutputStream(), key);
    PrefixIo.writePrefixLengthBytes(s.getOutputStream(), value);

    setCount.getAndIncrement();

    if (setKeys != null) {
      setKeys.add(key.copy());
    }
  }

  private void performGet(Socket s) throws IOException {
    randomizeKeyAndComputeValue();
    LOGGER.finest(() -> "get key " + key);

    s.getOutputStream().write(PrefixIo.GET);
    PrefixIo.writePrefixLengthBytes(s.getOutputStream(), key);

    getCount.getAndIncrement();

    int result = s.getInputStream().read();
    switch (result) {
      case -1:
        LOGGER.warning("unexpected end of stream");
        return;
      case PrefixIo.FOUND:
        findCount.getAndIncrement();
        var readValue = PrefixIo.readPrefixLengthBytes(s.getInputStream());
        LOGGER.finest(() -> String.format("found %s=%s", key, readValue));
        if (!readValue.equals(value)) {
          LOGGER.warning(() -> String.format("incorrect value for %s: expected %s but given %s",
              key, value, readValue));
        }
        break;
      case PrefixIo.NOT_FOUND:
        LOGGER.finest("not found");
        if (setKeys != null && setKeys.contains(key)) {
          missingKeyCount.getAndIncrement();
        }
        break;
      default:
        LOGGER.warning("bad result: " + result);
    }
  }

  private void randomizeKeyAndComputeValue() {
    var k = key.getArray();
    random.nextBytes(k);

    // for each key, deterministically compute a companion value
    var v = value.getArray();
    for (int i = 0; i < v.length; i++) {
      v[i] = (byte) ~(int) k[i % k.length];
    }
  }
}
