package dbf0.base;

import com.google.common.base.Preconditions;
import dbf0.Dbf0Util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class BaseServer extends Thread {

  private static final Logger LOGGER = Dbf0Util.getLogger(BaseServer.class);
  public static int BACKLOG = 50;

  private final InetSocketAddress bindAddress;
  private final int nThreads;
  private final AtomicReference<ServerSocket> serverSocket;

  protected BaseServer(InetSocketAddress bindAddress, int nThreads) {
    super("server");
    Preconditions.checkArgument(nThreads > 0);
    this.bindAddress = bindAddress;
    this.nThreads = nThreads;
    this.serverSocket = new AtomicReference<>();
  }

  abstract protected void processConnection(Socket socket) throws IOException;

  private void processConnectionInternal(Socket socket) {
    LOGGER.log(Level.FINER, () -> "processing connection " + socket + " in " + this);
    try (socket) {
      processConnection(socket);
    } catch (IOException e) {
      LOGGER.log(Level.WARNING, e, () -> "Unexpected error in processing connection: " + this);
    }
  }

  @Override
  public void run() {
    LOGGER.log(Level.INFO, "starting server " + this);
    Preconditions.checkState(serverSocket.get() == null);
    try {
      try {
        LOGGER.log(Level.FINE, "connecting to " + bindAddress);
        serverSocket.set(new ServerSocket(bindAddress.getPort(), BACKLOG, bindAddress.getAddress()));
        if (nThreads == 1) {
          runSingleThreaded();
        } else {
          runMultiThreaded();
        }
      } catch (SocketException e) {
        if (serverSocket.get() != null) {
          throw e;
        }
      } finally {
        closeServerSocket();
      }
    } catch (Exception e) {
      LOGGER.log(Level.WARNING, e, () -> "Unexpected error in server " + this);
    }
  }

  public void closeServerSocket() {
    var s = serverSocket.getAndSet(null);
    if (s != null) {
      try {
        s.close();
      } catch (IOException e) {
        LOGGER.log(Level.WARNING, e, () -> "Error closing socket in " + this);
      }
    }
  }

  private void runSingleThreaded() throws IOException {
    LOGGER.log(Level.FINE, "running single threaded server " + this);
    while (!interrupted()) {
      var s = this.serverSocket.get();
      if (s == null) {
        break;
      }
      processConnectionInternal(s.accept());
    }
  }

  private void runMultiThreaded() throws IOException {
    LOGGER.log(Level.FINE, "running " + nThreads + " threaded server " + this);
    var executor = Executors.newFixedThreadPool(nThreads, (r) -> {
      LOGGER.log(Level.FINE, "Starting server thread");
      return new Thread(r);
    });
    try {
      while (!interrupted()) {
        var s = this.serverSocket.get();
        if (s == null) {
          break;
        }
        var socket = s.accept();
        executor.execute(() -> processConnectionInternal(socket));
      }
    } finally {
      LOGGER.log(Level.FINE, "Shutting down thread pool");
      executor.shutdownNow();
      try {
        executor.awaitTermination(1, TimeUnit.SECONDS);
      } catch (InterruptedException ignore) {
      }
    }
  }
}
