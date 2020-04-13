package dbf0.socketer_server;

import org.apache.commons.codec.binary.Hex;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static dbf0.Dbf0Util.readArrayFully;
import static dbf0.socketer_server.Constants.*;

/**
 * Code stubs for use in project web page. Not actually run in benchmarking.
 */
class Stubs extends Thread {

  private static final int N_THREADS = 10;

  public static void runServerSingleThreaded() throws Exception {
    try (var serverSocket = new ServerSocket(PORT, 50, InetAddress.getByName(HOST))) {
      while (true) { // break out of loop when ServerSocket.accept throws IOException
        try (var socket = serverSocket.accept()) {
          var buffer = new byte[MSG_LENGTH];
          readArrayFully(socket.getInputStream(), buffer);
          socket.getOutputStream().write(buffer);
        }
      }
    }
  }

  public static void runServerMultiThreaded() throws Exception {
    var executor = Executors.newFixedThreadPool(N_THREADS);
    try {
      try (var serverSocket = new ServerSocket(PORT, 50, InetAddress.getByName(HOST))) {
        while (true) { // break out of loop when ServerSocket.accept throws IOException
          var socket = serverSocket.accept();
          executor.execute(() -> processConnection(socket));
        }
      }
    } finally {
      executor.shutdownNow();
      executor.awaitTermination(1, TimeUnit.SECONDS);
    }
  }

  private static void processConnection(Socket socket) {
    try (socket) {
      var buffer = new byte[MSG_LENGTH];
      readArrayFully(socket.getInputStream(), buffer);
      socket.getOutputStream().write(buffer);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }


  public static void runClientSingleThreaded() throws Exception {
    var sendMsg = new byte[MSG_LENGTH];
    var recvMsg = new byte[MSG_LENGTH];
    var random = new Random();
    while (true) { // run unit process is terminated
      random.nextBytes(sendMsg);
      try (var socket = new Socket(Constants.HOST, dbf0.socketer_server.Constants.PORT)) {
        socket.getOutputStream().write(sendMsg);
        readArrayFully(socket.getInputStream(), recvMsg);
      }
      if (!Arrays.equals(sendMsg, recvMsg)) {
        throw new RuntimeException(String.format("Inconsistent messages %s %s",
            Hex.encodeHexString(sendMsg), Hex.encodeHexString(recvMsg)));
      }
    }
  }

  public static void runClientMultiThreaded() throws Exception {
    var threads = IntStream.range(0, N_THREADS)
        .mapToObj(i -> new Thread(() -> {
          var random = new Random();
          var sendMsg = new byte[Constants.MSG_LENGTH];
          var recvMsg = new byte[Constants.MSG_LENGTH];
          try {
            while (true) { // run unit process is terminated
              random.nextBytes(sendMsg);
              try (var socket = new Socket(Constants.HOST, dbf0.socketer_server.Constants.PORT)) {
                socket.getOutputStream().write(sendMsg);
                readArrayFully(socket.getInputStream(), recvMsg);
              }
              if (!Arrays.equals(sendMsg, recvMsg)) {
                throw new RuntimeException(String.format("Inconsistent messages %s %s",
                    Hex.encodeHexString(sendMsg), Hex.encodeHexString(recvMsg)));
              }
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        })).collect(Collectors.toList());

    threads.forEach(Thread::start);

    for (var thread : threads) {
      thread.join();
    }
  }
}
