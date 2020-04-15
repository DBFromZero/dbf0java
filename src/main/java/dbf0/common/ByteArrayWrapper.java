package dbf0.common;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.apache.commons.codec.binary.Hex;

import java.util.Arrays;

public class ByteArrayWrapper {

  private final byte[] array;

  public ByteArrayWrapper(byte[] array) {
    this.array = Preconditions.checkNotNull(array);
  }

  public static ByteArrayWrapper of(byte[] array) {
    return new ByteArrayWrapper(array);
  }

  public static ByteArrayWrapper of(int... ints) {
    var bytes = new byte[ints.length];
    for (int i = 0; i < ints.length; i++) {
      bytes[i] = (byte) ints[i];
    }
    return of(bytes);
  }

  public byte[] getArray() {
    return array;
  }

  public static ByteArrayWrapper cat(ByteArrayWrapper... bws) {
    var total = Arrays.stream(bws).mapToInt(ByteArrayWrapper::length).sum();
    var bytes = new byte[total];
    int offset = 0;
    for (ByteArrayWrapper bw : bws) {
      var a = bw.getArray();
      System.arraycopy(a, 0, bytes, offset, a.length);
      offset += a.length;
    }
    return new ByteArrayWrapper(bytes);
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

  public int length() {
    return array.length;
  }

  public ByteArrayWrapper copy() {
    return new ByteArrayWrapper(Arrays.copyOf(array, array.length));
  }

  @Override
  public String toString() {
    if (length() <= 16) {
      return Hex.encodeHexString(array);
    } else {
      return MoreObjects.toStringHelper(this)
          .add("prefix", slice(0, 8).toString())
          .add("length", length())
          .add("hash", hashCode())
          .toString();
    }
  }

  public ByteArrayWrapper slice(int start, int end) {
    Preconditions.checkArgument(start >= 0);
    Preconditions.checkArgument(end >= 0);
    Preconditions.checkArgument(end <= length());
    Preconditions.checkArgument(start <= end);
    var s = new byte[end - start];
    System.arraycopy(getArray(), start, s, 0, s.length);
    return new ByteArrayWrapper(s);
  }
}
