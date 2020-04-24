package dbf0.disk_key_value.readwrite;

import java.io.Closeable;

public interface CloseableReadWriteStorage<K, V> extends ReadWriteStorage<K, V>, Closeable {
}
