package dbf0.disk_key_value.io;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import io.vavr.control.Either;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class MemoryFileDirectoryOperations implements FileDirectoryOperations<MemoryFileOperations.MemoryOutputStream> {

  private final String name;

  private boolean exists = false;
  private final ConcurrentMap<String, Either<MemoryFileOperations, MemoryFileDirectoryOperations>> children = new ConcurrentHashMap<>();

  public MemoryFileDirectoryOperations() {
    this("<unnamed>");
  }

  public MemoryFileDirectoryOperations(String name) {
    this.name = name;
  }

  public MemoryFileDirectoryOperations(String name, Map<String, Either<MemoryFileOperations, MemoryFileDirectoryOperations>> children) {
    this(name);
    this.children.putAll(children);
  }

  @Override public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("exists", exists)
        .add("children", children)
        .toString();
  }

  @Override public boolean exists() {
    return exists;
  }

  @Override public List<String> list() {
    return children.entrySet().stream().filter(
        e -> e.getValue().isLeft() ? e.getValue().getLeft().exists() : e.getValue().get().exists)
        .map(Map.Entry::getKey).collect(Collectors.toList());
  }

  @Override public void mkdirs() throws IOException {
    exists = true;
  }

  @Override public void clear() throws IOException {
    for (var entry : children.values()) {
      if (entry.isLeft()) {
        entry.getLeft().delete();
      } else {
        entry.get().clear();
      }
    }
  }

  @Override public FileOperations<MemoryFileOperations.MemoryOutputStream> file(String name) {
    Preconditions.checkArgument(!name.contains("/"));
    var entry = children.get(name);
    if (entry == null) {
      entry = Either.left(new MemoryFileOperations(name));
      children.put(name, entry);
    } else if (entry.isRight()) {
      throw new IllegalArgumentException(name + " is a directory");
    }
    return entry.getLeft();
  }

  @Override public MemoryFileDirectoryOperations subDirectory(String name) {
    Preconditions.checkArgument(!name.contains("/"));
    var entry = children.get(name);
    if (entry == null) {
      entry = Either.right(new MemoryFileDirectoryOperations(name));
      children.put(name, entry);
    } else if (entry.isLeft()) {
      throw new IllegalArgumentException(name + " is a directory");
    }
    return entry.get();
  }
}
