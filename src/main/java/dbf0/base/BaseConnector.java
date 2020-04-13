package dbf0.base;

import com.google.common.base.Preconditions;
import dbf0.Dbf0Util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class BaseConnector extends Thread {

  private static final Logger LOGGER = Dbf0Util.getLogger(BaseConnector.class);

  private final InetSocketAddress connectAddress;
  private final AtomicReference<Socket> socket;

  public static <T extends BaseConnector> List<T> createNConnectors(int n,
                                                                    Function<String, T> connectorFactory) {
    return IntStream.range(0, n)
        .mapToObj(i -> connectorFactory.apply("connector-" + i))
        .collect(Collectors.toList());
  }

  protected BaseConnector(InetSocketAddress connectAddress, String name) {
    super(name);
    this.connectAddress = connectAddress;
    this.socket = new AtomicReference<>();
  }

  abstract protected void processConnection(Socket s) throws IOException;

  @Override
  public void run() {
    LOGGER.log(Level.FINE, "Starting connector: " + this);
    try {
      while (!interrupted()) {
        LOGGER.log(Level.FINER, () -> "Connecting to: " + connectAddress);
        var newSocket = new Socket(connectAddress.getAddress(), connectAddress.getPort());
        var oldSocket = socket.getAndSet(newSocket);
        try {
          Preconditions.checkState(oldSocket == null);
          processConnection(newSocket);
        } catch (SocketException e) {
          if (socket.get() != null) {
            throw e;
          }
          break;
        } finally {
          closeSocketInternal();
        }
      }
    } catch (Exception e) {
      LOGGER.log(Level.WARNING, e, () -> "Unexpected error in connector: " + this);
    }
  }

  private void closeSocketInternal() throws IOException {
    var s = socket.getAndSet(null);
    if (s != null) {
      LOGGER.log(Level.FINER, () -> "Closing socket for: " + this);
      s.close();
    }
  }

  public void closeSocket() {
    try {
      closeSocketInternal();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
