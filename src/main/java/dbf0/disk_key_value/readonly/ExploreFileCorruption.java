package dbf0.disk_key_value.readonly;

import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.IoConsumer;
import dbf0.common.PrefixIo;
import org.apache.commons.lang3.tuple.Pair;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class ExploreFileCorruption {

  public static void main(String[] args) throws Exception {
    System.out.println("Reading index");
    var index = Dbf0Util.iteratorStream(new KeyValueFileIterator(new KeyValueFileReader("/data/tmp/sorted_kv_files/index100")))
        .collect(Collectors.toMap(Pair::getKey, entry -> ByteBuffer.wrap(entry.getValue().getArray()).getLong()));
    var size = new AtomicLong(0);
    var reader = new KeyValueFileIterator(new KeyValueFileReader("/data/tmp/sorted_kv_files/merged"));
    Dbf0Util.iteratorStream(reader).limit(10000).forEach(IoConsumer.wrap(entry -> {
      var indexSize = index.get(entry.getKey());
      if (indexSize != null) {
        if (indexSize != size.get()) {
          System.out.println("Bad index " + indexSize + " expected " + size.get());
        } else {
          System.out.println("Good index " + indexSize);
        }
      }
      size.addAndGet(serSize(entry.getKey()) + serSize(entry.getValue()));
    }));
  }

  private static int serSize(ByteArrayWrapper bw) throws IOException {
    var s = new ByteArrayOutputStream();
    PrefixIo.writeLength(s, bw.length());
    return s.size() + bw.length();
  }
}
