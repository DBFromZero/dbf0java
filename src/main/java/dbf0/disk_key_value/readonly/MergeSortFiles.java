package dbf0.disk_key_value.readonly;

import com.google.common.collect.Iterators;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.IoFunction;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static dbf0.disk_key_value.readonly.WriteSortedKeyValueFiles.DIRECTORY;
import static dbf0.disk_key_value.readonly.WriteSortedKeyValueFiles.FILES;

public class MergeSortFiles {

  public static void main(String[] args) throws Exception {
    var iterators = IntStream.range(0, FILES).boxed().map(IoFunction.wrap(index ->
        new KeyValueFileIterator(new KeyValueFileReader(DIRECTORY + "/" + index))))
        .collect(Collectors.toList());
    var sortedIterator = Iterators.mergeSorted(iterators, Map.Entry.comparingByKey());
    ByteArrayWrapper lastKey = null;
    var storage = new BasicKeyValueStorage(DIRECTORY + "/merged");
    storage.initialize();
    int i = 0;
    while (sortedIterator.hasNext()) {
      var entry = sortedIterator.next();
      if (i % 10000 == 0) {
        System.out.println("Writing " + i);
      }
      i++;
      if (lastKey != null && lastKey.equals(entry.getKey())) {
        System.out.println("Skipping duplicate key");
        continue;
      }
      lastKey = entry.getKey();
      storage.store(entry.getKey(), entry.getValue());
    }
    storage.close();
  }

}
