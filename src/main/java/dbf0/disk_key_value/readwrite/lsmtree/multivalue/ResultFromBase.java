package dbf0.disk_key_value.readwrite.lsmtree.multivalue;

import dbf0.common.io.IOIterator;
import dbf0.disk_key_value.readonly.multivalue.MultiValueResult;
import dbf0.disk_key_value.readwrite.MultiValueReadWriteStorage;

import java.io.IOException;

class ResultFromBase<V> implements MultiValueReadWriteStorage.Result<V> {

  private final MultiValueResult<ValueWrapper<V>> result;

  public ResultFromBase(MultiValueResult<ValueWrapper<V>> result) {
    this.result = result;
  }

  @Override public IOIterator<V> iterator() {
    return result.valueIterator().map(ValueWrapper::getValue);
  }

  @Override public void close() throws IOException {
    result.close();
  }

  @Override public int knownSize() {
    return result.count();
  }

  @Override public int maxSize() {
    return result.count();
  }
}
