package dbf0.document.misc;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;
import dbf0.document.serialization.DElementSerializer;
import dbf0.document.types.DInt;
import dbf0.document.types.DMap;
import dbf0.document.types.DString;

import java.util.LinkedList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ShowSerializedDocument {

  public static void main(String[] args) throws Exception {
    var element = DMap.of(DInt.of(5), DString.of("hello"));
    var serializer = DElementSerializer.defaultCharsetInstance();
    var bytes = serializer.serializeToBytes(element);
    printSerialized(bytes);
  }

  public static void printSerialized(ByteArrayWrapper bytes) {
    Preconditions.checkNotNull(bytes);
    Preconditions.checkArgument(bytes.length() >= 1);
    int first = bytes.getArray()[0];
    System.out.println(String.format("%s %s %s", binaryFormat(first & 0xF), binaryFormat(first >>> 4),
        spacedHex(bytes.slice(1, bytes.length()))));
  }

  public static String binaryFormat(int x) {
    return Joiner.on("").join(
        IntStream.range(0, 4).mapToObj(i -> (x & (1 << i)) > 0 ? "1" : "0")
            .collect(Collectors.toCollection(LinkedList::new))
            .descendingIterator()
    );
  }

  public static String spacedHex(ByteArrayWrapper bytes) {
    var hex = bytes.hexString();
    var n = hex.length();
    var builder = new StringBuilder(n + n / 2);
    for (int i = 0; i < n; i += 2) {
      if (i > 0) builder.append(" ");
      builder.append(hex, i, i + 2);
    }
    return builder.toString();
  }
}
