import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Benchmark {

  private static class Spec {
    private final int serverThreads;
    private final int clientThreads;
    private final int trialIndex;

    public Spec(int serverThreads, int clientThreads, int trialIndex) {
      this.serverThreads = serverThreads;
      this.clientThreads = clientThreads;
      this.trialIndex = trialIndex;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("serverThreads", serverThreads)
          .add("clientThreads", clientThreads)
          .add("trialIndex", trialIndex)
          .toString();
    }
  }

  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 4);
    var serverThreadsList = parseThreadsList(args[0]);
    var clientThreadsList = parseThreadsList(args[1]);
    var trials = Integer.parseInt(args[2]);
    var duration = Duration.parse(args[3]);

    var specs = serverThreadsList.stream().flatMap(serverThreads ->
        clientThreadsList.stream().flatMap(clientThreads ->
            IntStream.range(0, trials).mapToObj(trialIndex ->
                new Spec(serverThreads, clientThreads, trialIndex))))
        .collect(Collectors.toList());
    Collections.shuffle(specs);

    var results = specs.stream().map(spec -> Util.callUnchecked(() -> {
      System.out.println("Running: " + spec);
      var server = new Server(spec.serverThreads);
      server.start();
      Thread.sleep(100);

      var client = new Client(spec.clientThreads, duration);
      client.start();
      client.join();
      var messages = client.getMsgCount();

      System.out.println("Messages " + messages);
      server.closeServerSocket();
      server.join();

      return ImmutableMap.of(
          "serverThreads", spec.serverThreads,
          "clientThreads", spec.clientThreads,
          "trialIndex", spec.trialIndex,
          "messages", messages);
    })).collect(Collectors.toList());

    System.out.println(new Gson().toJson(ImmutableMap.of("duration", duration.toString(), "results", results)));
  }

  private static ImmutableList<Integer> parseThreadsList(String s) {
    return Arrays.stream(s.split("[,\\s+]"))
        .map(Integer::parseInt)
        .collect(ImmutableList.toImmutableList());
  }
}
