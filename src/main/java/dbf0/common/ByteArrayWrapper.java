package dbf0.common;

import com.google.common.base.Preconditions;
import org.apache.commons.codec.binary.Hex;

import java.util.Arrays;

public class ByteArrayWrapper {

  private final byte[] array;

  public ByteArrayWrapper(byte[] array) {
    this.array = Preconditions.checkNotNull(array);
  }

  public byte[] getArray() {
    return array;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ByteArrayWrapper that = (ByteArrayWrapper) o;
    return Arrays.equals(array, that.array);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(array);
  }

  @Override
  public String toString() {
    return Hex.encodeHexString(array);
  }

  public ByteArrayWrapper copy() {
    return new ByteArrayWrapper(Arrays.copyOf(array, array.length));
  }
}
