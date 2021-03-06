package dbf0.mem_key_value;

import com.google.common.base.Preconditions;
import dbf0.base.BaseServer;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.io.IOUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

class KeyValueServer extends BaseServer {

  private static final Logger LOGGER = Dbf0Util.getLogger(KeyValueServer.class);

  private final Map<ByteArrayWrapper, ByteArrayWrapper> map;

  KeyValueServer(InetSocketAddress bindAddress, int nThreads, Map<ByteArrayWrapper, ByteArrayWrapper> map) {
    super(bindAddress, nThreads);
    this.map = Preconditions.checkNotNull(map);
  }

  static KeyValueServer hashMapKeyValueServer(InetSocketAddress bindAddress, int nThreads) {
    return new KeyValueServer(bindAddress, nThreads, new HashMap<>());
  }

  static KeyValueServer hashTableKeyValueServer(InetSocketAddress bindAddress, int nThreads) {
    return new KeyValueServer(bindAddress, nThreads, new Hashtable<>());
  }

  static KeyValueServer concurrentHashMapKeyValueStore(InetSocketAddress bindAddress, int nThreads) {
    return new KeyValueServer(bindAddress, nThreads, new ConcurrentHashMap<>());
  }

  int size() {
    return map.size();
  }

  @Override
  protected void processConnection(Socket socket) throws IOException {
    int operation = socket.getInputStream().read();
    switch (operation) {
      case -1:
        LOGGER.warning("unexpected end of stream");
        return;
      case KeyValueConnector.SET:
        processSet(socket);
        break;
      case KeyValueConnector.GET:
        processGet(socket);
        break;
      default:
        LOGGER.warning("bad operation: " + operation);
    }
  }

  private void processSet(Socket socket) throws IOException {
    var key = IOUtil.readBytes(socket.getInputStream());
    var value = IOUtil.readBytes(socket.getInputStream());
    LOGGER.finest(() -> "Set " + key + " to " + value);
    map.put(key, value);
  }

  private void processGet(Socket socket) throws IOException {
    var key = IOUtil.readBytes(socket.getInputStream());
    var value = map.get(key);
    if (value == null) {
      LOGGER.finest(() -> "No value for " + key);
      socket.getOutputStream().write(KeyValueConnector.NOT_FOUND);
    } else {
      LOGGER.finest(() -> "Found value for " + key + " of " + value);
      socket.getOutputStream().write(KeyValueConnector.FOUND);
      IOUtil.writeBytes(socket.getOutputStream(), value);
    }
  }
}
