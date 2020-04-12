import com.google.common.base.Preconditions;
import org.apache.commons.codec.binary.Hex;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class Server extends Thread {

  public static final String HOST = "localhost";
  public static int PORT = 8080;
  public static int MSG_LENGTH = 32;

  private final int nThreads;
  private volatile AtomicReference<ServerSocket> serverSocket = null;

  public Server(int nThreads) {
    super("server");
    Preconditions.checkArgument(nThreads > 0);
    this.nThreads = nThreads;
  }

  @Override
  public void run() {
    try {
      try {
        serverSocket = new AtomicReference<>(new ServerSocket(PORT, 50, InetAddress.getByName(HOST)));
        System.out.println("Server is listening ");
        if (nThreads == 1) {
          runSingleThreaded();
        } else {
          runMultithreaded();
        }
      } catch (SocketException e) {
        if (serverSocket.get() != null) {
          throw e;
        }
      } finally {
        closeServerSocket();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void closeServerSocket() {
    var s = serverSocket.getAndSet(null);
    if (s != null) {
      try {
        s.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private void runSingleThreaded() throws IOException {
    while (!Thread.interrupted()) {
      var s = this.serverSocket.get();
      if (s == null) {
        break;
      }
      processConnection(s.accept());
    }
  }

  private void runMultithreaded() throws IOException {
    var executor = Executors.newFixedThreadPool(nThreads, (r) -> {
      System.out.println("Starting thread");
      return new Thread(r);
    });
    try {
      while (!Thread.interrupted()) {
        var s = this.serverSocket.get();
        if (s == null) {
          break;
        }
        var socket = s.accept();
        executor.execute(() -> processConnection(socket));
      }
    } finally {
      executor.shutdownNow();
      try {
        executor.awaitTermination(1, TimeUnit.SECONDS);
      } catch (InterruptedException ignore) {
      }
    }
  }

  private void processConnection(Socket socket) {
    try (socket) {
      var buffer = new byte[MSG_LENGTH];
      Util.readArrayFully(socket.getInputStream(), buffer);
      socket.getOutputStream().write(buffer);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
