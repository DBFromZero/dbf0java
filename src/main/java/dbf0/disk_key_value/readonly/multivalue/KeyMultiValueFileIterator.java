package dbf0.disk_key_value.readonly.multivalue;

import com.google.common.base.Preconditions;
import dbf0.common.io.IOIterator;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;

public class KeyMultiValueFileIterator<K, V> implements IOIterator<Pair<K, MultiValueResult<V>>> {
  private final KeyMultiValueFileReader<K, V> reader;
  private final MutablePair<K, MultiValueResult<V>> pair;
  private boolean hasNext = false;

  public KeyMultiValueFileIterator(KeyMultiValueFileReader<K, V> reader) {
    this.reader = Preconditions.checkNotNull(reader);
    pair = MutablePair.of(null,
        new MultiValueResultImp<>(reader) {
          @Override public void close() {
          }
        });
  }

  @Override public boolean hasNext() throws IOException {
    if (!hasNext) {
      var key = reader.readKey();
      if (key == null) {
        return false;
      }
      pair.setLeft(key);
    }
    return true;
  }

  @Override public Pair<K, MultiValueResult<V>> next() throws IOException {
    if (!hasNext && !hasNext()) {
      throw new RuntimeException("no next");
    }
    hasNext = false;
    return pair;
  }
}
