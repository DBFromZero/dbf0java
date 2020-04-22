package dbf0.disk_key_value.readwrite.btree;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import dbf0.disk_key_value.readwrite.ReadWriteStorage;

import java.util.stream.Stream;

public interface BTree<K extends Comparable<K>, V> extends ReadWriteStorage<K, V> {

  @VisibleForTesting Stream<Long> streamIdsInUse();

  @VisibleForTesting BTreeStorage<K, V> getStorage();

  @VisibleForTesting static Stream<Long> streamIdsInUseHelper(Node<?, ?> root) {
    return Streams.concat(
        Stream.of(root.getId()),
        Stream.of(root)
            .filter(ParentNode.class::isInstance)
            .map(ParentNode.class::cast)
            .flatMap(ParentNode::streamIdsInUse)
    );
  }
}
