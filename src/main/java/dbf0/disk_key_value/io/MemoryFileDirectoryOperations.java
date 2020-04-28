package dbf0.disk_key_value.io;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class MemoryFileDirectoryOperations implements FileDirectoryOperations<MemoryFileOperations.MemoryOutputStream> {

  private final ConcurrentMap<String, MemoryFileOperations> map = new ConcurrentHashMap<>();

  @Override public boolean exists() {
    return true;
  }

  @Override public List<String> list() throws IOException {
    return map.entrySet().stream().filter(e -> e.getValue().exists())
        .map(Map.Entry::getKey).collect(Collectors.toList());
  }

  @Override public void mkdirs() throws IOException {
  }

  @Override public void clear() throws IOException {
    for (var file : map.values()) {
      if (file.exists()) {
        file.delete();
      }
    }
  }

  @Override public FileOperations<MemoryFileOperations.MemoryOutputStream> file(String name) {
    Preconditions.checkArgument(!name.contains("/"));
    var file = map.get(name);
    if (file == null) {
      file = new MemoryFileOperations(name);
      map.put(name, file);
    }
    return file;
  }
}
