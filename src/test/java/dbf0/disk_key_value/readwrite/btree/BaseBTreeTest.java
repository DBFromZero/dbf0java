package dbf0.disk_key_value.readwrite.btree;

import com.google.common.base.Joiner;
import dbf0.common.Dbf0Util;
import dbf0.disk_key_value.readwrite.ReadWriteStorageTester;
import dbf0.test.*;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.logging.Level;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

abstract class BaseBTreeTest {

  abstract protected BTree<Integer, Integer> bTree() throws IOException;

  protected boolean isDebug() {
    return false;
  }

  protected void print(Object... args) {
    if (isDebug()) {
      System.out.println(Joiner.on(" ").join(args));
    }
  }

  @Before public void setUp() {
    Dbf0Util.enableConsoleLogging(Level.WARNING, true);
  }

  @Test public void testEmpty() throws IOException {
    var btree = bTree();
    assertThat(btree.size()).isEqualTo(0);
    assertThat(btree.get(0)).isNull();
    validateIdsInUse(btree);
  }

  @Test public void testPutSingle() throws IOException {
    var btree = bTree();
    btree.put(1, 2);
    assertThat(btree.size()).isEqualTo(1);
    assertThat(btree.get(1)).isEqualTo(2);
    validateIdsInUse(btree);
  }

  @Test public void testDeleteEmpty() throws IOException {
    var btree = bTree();
    var deleted = btree.delete(1);
    assertThat(deleted).isFalse();
    assertThat(btree.size()).isEqualTo(0);
    assertThat(btree.get(1)).isNull();
    validateIdsInUse(btree);
  }

  @Test public void testDeleteSingle() throws IOException {
    var btree = bTree();
    btree.put(1, 2);
    var deleted = btree.delete(1);
    assertThat(deleted).isTrue();
    assertThat(btree.size()).isEqualTo(0);
    assertThat(btree.get(1)).isNull();
    validateIdsInUse(btree);
  }

  @Test public void testReplaceSingle() throws IOException {
    var btree = bTree();
    btree.put(1, 2);
    assertThat(btree.size()).isEqualTo(1);
    assertThat(btree.get(1)).isEqualTo(2);
    btree.put(1, 3);
    assertThat(btree.size()).isEqualTo(1);
    assertThat(btree.get(1)).isEqualTo(3);
    validateIdsInUse(btree);
  }

  @Test public void testAddDeleteMany(RandomSeed seed, Count count, KeySetSize keySetSize) throws IOException {
    var btree = bTree();

    ReadWriteStorageTester.builderForIntegers(btree, seed, keySetSize)
        .debug(isDebug())
        .build()
        .testAddDeleteMany(count.count);

    var idsList = btree.streamIdsInUse().collect(Collectors.toList());
    assertThat(idsList).hasSize(1);
    assertThat(btree.getStorage().getIdsInUse()).hasSize(1);
    assertThat(idsList).hasSameElementsAs(btree.getStorage().getIdsInUse());
  }

  @Test public void testPutDeleteGet(RandomSeed seed, Count count, KeySetSize keySetSize,
                                     PutDeleteGet putDeleteGet, KnownKeyRate knownKeyRate) throws IOException {
    var btree = bTree();
    ReadWriteStorageTester.builderForIntegers(btree, seed, keySetSize)
        .debug(isDebug())
        .iterationCallback((ignored) -> validateIdsInUse(btree))
        .build()
        .testPutDeleteGet(count.count, putDeleteGet, knownKeyRate);
  }

  @Test public void testLarge() throws IOException {
    var btree = bTree();
    Assume.assumeTrue(btree.getStorage().getConfig().getLeafCapacity() == 4);

    ReadWriteStorageTester.builderForIntegers(btree, RandomSeed.CAFE, KeySetSize.S1000)
        .debug(isDebug())
        .iterationCallback((ignored) -> validateIdsInUse(btree))
        .build()
        .testPutDeleteGet(100 * 1000, PutDeleteGet.BALANCED, KnownKeyRate.MID);
  }

  protected void validateIdsInUse(BTree<Integer, Integer> btree) throws IOException {
    var idsList = btree.streamIdsInUse().collect(Collectors.toList());
    assertThat(idsList).doesNotHaveDuplicates();
    assertThat(idsList).hasSameElementsAs(btree.getStorage().getIdsInUse());
  }
}
