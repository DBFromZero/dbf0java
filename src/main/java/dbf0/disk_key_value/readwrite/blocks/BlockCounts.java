package dbf0.disk_key_value.readwrite.blocks;

import com.google.common.base.MoreObjects;
import dbf0.common.Dbf0Util;

import java.util.Objects;

public class BlockCounts {

  private final int blocks;
  private final long bytes;

  public BlockCounts(int blocks, long bytes) {
    this.blocks = blocks;
    this.bytes = bytes;
  }

  public int getBlocks() {
    return blocks;
  }

  public long getBytes() {
    return bytes;
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BlockCounts that = (BlockCounts) o;
    return blocks == that.blocks &&
        bytes == that.bytes;
  }

  @Override public int hashCode() {
    return Objects.hash(blocks, bytes);
  }

  @Override public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("blocks", blocks)
        .add("bytes", Dbf0Util.formatBytes(bytes))
        .toString();
  }
}
