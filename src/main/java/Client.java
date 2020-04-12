import com.google.common.base.Preconditions;
import org.apache.commons.codec.binary.Hex;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Client extends Thread {

  private final int nThreads;
  private final Duration duration;
  private final AtomicInteger msgCount = new AtomicInteger(0);

  public Client(int nThreads, Duration duration) {
    super("client");
    this.nThreads = nThreads;
    this.duration = duration;
  }

  @Override
  public void run() {
    System.out.println("Starting client with " + nThreads + " threads");
    var connectors = IntStream.range(0, nThreads).mapToObj(i -> new Connector("thread-" + i)).collect(Collectors.toList());
    connectors.forEach(Connector::start);

    System.out.println("Sleeping");
    try {
      Thread.sleep(duration.toMillis());
      connectors.forEach(Thread::interrupt);
      connectors.forEach(Connector::safelyCloseSocket);
      for (var thread : connectors) {
        thread.join();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public int getMsgCount() {
    return msgCount.get();
  }

  private class Connector extends Thread {

    private final Random random;
    private final AtomicReference<Socket> socket;

    public Connector(String name) {
      super(name);
      this.random = new Random();
      this.socket = new AtomicReference<>(null);
    }

    @Override
    public void run() {
      var sendMsg = new byte[Server.MSG_LENGTH];
      var recvMsg = new byte[Server.MSG_LENGTH];
      try {
        while (!interrupted()) {
          random.nextBytes(sendMsg);
          var s = new Socket(Server.HOST, Server.PORT);
          var old = socket.getAndSet(s);
          Preconditions.checkState(old == null);
          try {
            s.getOutputStream().write(sendMsg);
            Util.readArrayFully(s.getInputStream(), recvMsg);
          } catch (SocketException e) {
            if (socket.get() == null) {
              return;
            } else {
              throw e;
            }
          } finally {
            closeSocket();
          }
          if (!Arrays.equals(sendMsg, recvMsg)) {
            throw new RuntimeException(String.format("Inconsistent messages %s %s",
                Hex.encodeHexString(sendMsg), Hex.encodeHexString(recvMsg)));
          }
          msgCount.incrementAndGet();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    private void closeSocket() throws IOException {
      var s = socket.getAndSet(null);
      if (s != null) {
        s.close();
      }
    }

    private void safelyCloseSocket() {
      try {
        closeSocket();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
