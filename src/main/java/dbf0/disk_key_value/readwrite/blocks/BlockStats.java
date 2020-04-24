package dbf0.disk_key_value.readwrite.blocks;

import com.google.common.base.MoreObjects;

import java.util.Objects;

public class BlockStats {

  private final BlockCounts used;
  private final BlockCounts unused;

  public BlockStats(BlockCounts used, BlockCounts unused) {
    this.used = used;
    this.unused = unused;
  }

  public BlockCounts getUsed() {
    return used;
  }

  public BlockCounts getUnused() {
    return unused;
  }

  public double unusedBytesFraction() {
    return (double) unused.getBytes() / (double) (unused.getBytes() + used.getBytes());
  }

  public long totalBytes() {
    return used.getBytes() + unused.getBytes();
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BlockStats that = (BlockStats) o;
    return Objects.equals(used, that.used) &&
        Objects.equals(unused, that.unused);
  }

  @Override public int hashCode() {
    return Objects.hash(used, unused);
  }

  @Override public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("used", used)
        .add("unused", unused)
        .add("percentUnused", String.format("%.1f%%", 100 * unusedBytesFraction()))
        .toString();
  }
}
