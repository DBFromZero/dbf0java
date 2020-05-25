package dbf0.disk_key_value.readonly.singlevalue;

import com.google.common.base.Preconditions;
import dbf0.common.io.IOIterator;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.NoSuchElementException;

public class KeyValueFileIterator<K, V> implements IOIterator<Pair<K, V>> {
  private final KeyValueFileReader<K, V> reader;
  private boolean hasReadNext = false;
  private Pair<K, V> next = null;

  public KeyValueFileIterator(KeyValueFileReader<K, V> reader) {
    this.reader = Preconditions.checkNotNull(reader);
  }

  @Override
  public boolean hasNext() throws IOException {
    if (!hasReadNext) {
      next = readNext();
      hasReadNext = next != null;
    }
    return hasReadNext;
  }

  @Override
  public Pair<K, V> next() throws IOException {
    if (!hasReadNext && !hasNext()) {
      throw new NoSuchElementException();
    }
    hasReadNext = false;
    var n = next;
    next = null;
    return n;
  }

  @Nullable
  private Pair<K, V> readNext() throws IOException {
    return reader.readKeyValue();
  }
}
