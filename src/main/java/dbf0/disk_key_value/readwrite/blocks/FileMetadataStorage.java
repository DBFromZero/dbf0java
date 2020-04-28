package dbf0.disk_key_value.readwrite.blocks;

import com.google.common.base.Preconditions;
import dbf0.disk_key_value.io.FileOperations;
import dbf0.disk_key_value.io.SerializationHelper;
import dbf0.disk_key_value.io.Serializer;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class FileMetadataStorage<T extends OutputStream> implements Closeable {

  private final FileOperations<T> fileOperations;
  private final Map<String, FileMetadataMapWriter<?, ?>> writers = new HashMap<>();

  private transient T outputStream;
  private transient SerializationHelper serializationHelper;
  private boolean pauseSync = false;

  public FileMetadataStorage(FileOperations<T> fileOperations) {
    this.fileOperations = fileOperations;
  }

  public void initialize() throws IOException {
    Preconditions.checkState(outputStream == null, "already initialized");
    outputStream = fileOperations.createAppendOutputStream();
    serializationHelper = new SerializationHelper(outputStream);
  }

  @Override public void close() throws IOException {
    Preconditions.checkState(outputStream != null, "not initialized");
    outputStream.close();
    outputStream = null;
    serializationHelper = null;
  }

  public <K, V> MetadataMap<K, V> newMap(String name, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    Preconditions.checkState(outputStream != null, "not initialized");
    Preconditions.checkArgument(!writers.containsKey(name), "already have a writer for name %s", name);

    var writer = new FileMetadataMapWriter<>(name, keySerializer, valueSerializer);
    writers.put(name, writer);
    return writer;
  }

  public static <T2 extends OutputStream> FileMetadataStorage<T2> load(FileOperations<T2> fileOperations) throws IOException {
    throw new RuntimeException("not implemented");
  }

  public void pauseSync() {
    pauseSync = true;
  }

  public void resumeSync() throws IOException {
    pauseSync = false;
    syncIo();
  }

  private class FileMetadataMapWriter<K, V> implements MetadataMap<K, V> {

    private static final byte PUT = (byte) 0x10;
    private static final byte DELETE = (byte) 0x20;
    private static final byte CLEAR = (byte) 0x30;

    private final String name;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final Map<K, V> map = new HashMap<>();

    public FileMetadataMapWriter(String name, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
      this.name = name;
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;
    }

    @Override public int size() {
      return map.size();
    }

    @Override public V put(K key, V value) throws IOException {
      putOne(key, value);
      syncIo();
      return map.put(key, value);
    }

    @Override public V get(K key) {
      return map.get(key);
    }

    @Override public boolean containsKey(K key) {
      return map.containsKey(key);
    }

    @Override public V delete(K key) throws IOException {
      writePrefix(DELETE);
      keySerializer.serialize(serializationHelper, key);
      syncIo();
      return map.remove(key);
    }

    @Override public void clear() throws IOException {
      writePrefix(CLEAR);
      syncIo();
      map.clear();
    }

    @Override public void putAll(Map<K, V> source) throws IOException {
      for (var entry : source.entrySet()) {
        putOne(entry.getKey(), entry.getValue());
      }
      syncIo();
      map.putAll(source);
    }

    @Override public Map<K, V> unmodifiableMap() {
      return Collections.unmodifiableMap(map);
    }

    private void putOne(K key, V value) throws IOException {
      writePrefix(PUT);
      keySerializer.serialize(serializationHelper, key);
      valueSerializer.serialize(serializationHelper, value);
    }

    private void writePrefix(byte operation) throws IOException {
      serializationHelper.writeString(name);
      serializationHelper.writeByte(operation);
    }
  }

  private void syncIo() throws IOException {
    if (!pauseSync) {
      fileOperations.sync(outputStream);
    }
  }
}
