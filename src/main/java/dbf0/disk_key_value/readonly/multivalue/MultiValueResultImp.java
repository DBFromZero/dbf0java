package dbf0.disk_key_value.readonly.multivalue;

import dbf0.common.io.IOIterator;

import java.io.IOException;

public class MultiValueResultImp<V> implements MultiValueResult<V> {
  private final KeyMultiValueFileReader<?, V> reader;

  MultiValueResultImp(KeyMultiValueFileReader<?, V> reader) {
    this.reader = reader;
  }

  @Override public int count() {
    return reader.getValuesCount();
  }

  @Override public int remaining() {
    return reader.getValuesRemaining();
  }

  @Override public V readValue() throws IOException {
    return reader.readValue();
  }

  @Override public void skipValue() throws IOException {
    reader.skipValue();
  }

  @Override public void skipRemainingValues() throws IOException {
    reader.skipRemainingValues();
  }

  @Override public IOIterator<V> valueIterator() {
    return reader.valueIterator();
  }

  @Override public void close() throws IOException {
    reader.close();
  }

  @Override public String toString() {
    return "MVR{" + reader.toString() + "}";
  }
}
