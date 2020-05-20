package dbf0.disk_key_value.readwrite.lsmtree.multivalue;

import dbf0.common.io.IOFunction;
import dbf0.common.io.IOIterator;
import dbf0.common.io.IOPredicate;
import dbf0.disk_key_value.readonly.multivalue.MultiValueResult;
import dbf0.disk_key_value.readwrite.MultiValueReadWriteStorage;

import java.io.IOException;
import java.util.function.Predicate;

class ResultRemovingDeletesSingleSource<V> implements MultiValueReadWriteStorage.Result<V> {

  private final MultiValueResult<ValueWrapper<V>> reader;

  public ResultRemovingDeletesSingleSource(MultiValueResult<ValueWrapper<V>> reader) {
    this.reader = reader;
  }

  @Override public IOIterator<V> iterator() {
    return reader.valueIterator()
        .filter(IOPredicate.of(Predicate.not(ValueWrapper::isDelete)))
        .map(IOFunction.of(ValueWrapper::getValue));
  }

  @Override public void close() throws IOException {
    reader.close();
  }

  @Override public int maxSize() {
    return reader.count();
  }
}
