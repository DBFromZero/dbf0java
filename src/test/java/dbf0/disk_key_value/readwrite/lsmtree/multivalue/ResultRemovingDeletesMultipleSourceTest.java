package dbf0.disk_key_value.readwrite.lsmtree.multivalue;

import dbf0.common.Dbf0Util;
import dbf0.common.io.IOIterator;
import dbf0.disk_key_value.readonly.multivalue.InMemoryMultiValueResult;
import dbf0.test.RandomSeed;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ResultRemovingDeletesMultipleSourceTest {

  private static final Logger LOGGER = Dbf0Util.getLogger(MultiValueSelectorIteratorTest.class);
  private static final Comparator<Integer> VALUE_COMPARATOR = Integer::compareTo;

  private static final ValueWrapper<Integer> PUT_1 = put(1);
  private static final ValueWrapper<Integer> PUT_2 = put(2);
  private static final ValueWrapper<Integer> DEL_1 = del(1);

  @Before public void setUp() throws Exception {
    Dbf0Util.enableConsoleLogging(Level.FINER, true);
  }

  @Test public void testEmpty() throws IOException {
    var result = result();
    assertThat(result.maxSize()).isZero();
    var iterator = result.iterator();
    assertThat(iterator.hasNext()).isFalse();
    assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test public void testSingle() throws IOException {
    var result = result(List.of(PUT_1));
    assertThat(result.maxSize()).isOne();
    var iterator = result.iterator();
    checkHasValue(iterator, PUT_1);
    assertThat(iterator.hasNext()).isFalse();
  }

  @Test public void tesTwoInOneResult() throws IOException {
    var result = result(List.of(PUT_1, PUT_2));
    assertThat(result.maxSize()).isEqualTo(2);
    var iterator = result.iterator();
    checkHasValue(iterator, PUT_1);
    checkHasValue(iterator, PUT_2);
    assertThat(iterator.hasNext()).isFalse();
  }

  @Test public void tesTwoInTwoResult() throws IOException {
    var result = result(List.of(PUT_1), List.of(PUT_2));
    assertThat(result.maxSize()).isEqualTo(2);
    var iterator = result.iterator();
    checkHasValue(iterator, PUT_1);
    checkHasValue(iterator, PUT_2);
    assertThat(iterator.hasNext()).isFalse();
  }

  @Test public void testDeduplicate() throws IOException {
    var result = result(List.of(PUT_1), List.of(PUT_1));
    assertThat(result.maxSize()).isEqualTo(2);
    var iterator = result.iterator();
    checkHasValue(iterator, PUT_1);
    assertThat(iterator.hasNext()).isFalse();
  }

  @Test public void testSingleDelete() throws IOException {
    var result = result(List.of(DEL_1));
    assertThat(result.maxSize()).isOne();
    var iterator = result.iterator();
    assertThat(iterator.hasNext()).isFalse();
  }

  @Test public void testNewerDelete() throws IOException {
    var result = result(List.of(DEL_1), List.of(PUT_1));
    assertThat(result.maxSize()).isEqualTo(2);
    var iterator = result.iterator();
    assertThat(iterator.hasNext()).isFalse();
  }

  @Test public void testOlderDelete() throws IOException {
    var result = result(List.of(PUT_1), List.of(DEL_1));
    assertThat(result.maxSize()).isEqualTo(2);
    var iterator = result.iterator();
    checkHasValue(iterator, PUT_1);
    assertThat(iterator.hasNext()).isFalse();
  }

  @Test public void testMiddleDelete() throws IOException {
    var result = result(List.of(PUT_1), List.of(DEL_1), List.of(PUT_1));
    assertThat(result.maxSize()).isEqualTo(3);
    var iterator = result.iterator();
    checkHasValue(iterator, PUT_1);
    assertThat(iterator.hasNext()).isFalse();
  }

  @Test public void testRandom() throws IOException {
    var random = RandomSeed.CAFE.random();
    var lists = IntStream.range(0, 10).mapToObj(ignored -> randomList(random)).collect(Collectors.toList());
    var expected = mergeLists(lists);
    var result = result(lists).realizeRemainingValues();
    assertThat(result).isSortedAccordingTo(VALUE_COMPARATOR);
    assertThat(result).doesNotHaveDuplicates();
    assertThat(result).containsExactlyElementsOf(expected);
  }

  private void checkHasValue(IOIterator<Integer> iterator, ValueWrapper<Integer> put) throws IOException {
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(put.getValue());
  }

  private static ValueWrapper<Integer> put(int i) {
    return new ValueWrapper<>(false, i);
  }

  private static ValueWrapper<Integer> del(int i) {
    return new ValueWrapper<>(true, i);
  }

  private static ResultRemovingDeletesMultipleSource<Integer> result(List<List<ValueWrapper<Integer>>> results) {
    return new ResultRemovingDeletesMultipleSource<>(results.stream().map(InMemoryMultiValueResult::new)
        .collect(Collectors.toList()), VALUE_COMPARATOR);
  }

  private static ResultRemovingDeletesMultipleSource<Integer> result(List<ValueWrapper<Integer>>... results) {
    return result(List.of(results));
  }

  private static List<ValueWrapper<Integer>> randomList(Random random) {
    return IntStream.range(0, 50 + random.nextInt(200))
        .map(ignored -> random.nextInt(2500))
        .distinct()
        .sorted()
        .mapToObj(value -> new ValueWrapper<>(random.nextFloat() < 0.2f, value))
        .collect(Collectors.toList());
  }

  private static List<Integer> mergeLists(List<List<ValueWrapper<Integer>>> results) {
    Set<Integer> combined = new HashSet<>(256);
    for (int i = results.size() - 1; i >= 0; i--) {
      for (var valueWrapper : results.get(i)) {
        if (valueWrapper.isDelete()) {
          combined.remove(valueWrapper.getValue());
        } else {
          combined.add(valueWrapper.getValue());
        }
      }
    }
    var sorted = new ArrayList<>(combined);
    sorted.sort(VALUE_COMPARATOR);
    return sorted;
  }
}
