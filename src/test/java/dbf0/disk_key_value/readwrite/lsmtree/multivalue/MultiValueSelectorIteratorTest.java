package dbf0.disk_key_value.readwrite.lsmtree.multivalue;

import com.google.common.base.MoreObjects;
import com.google.common.base.Predicates;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.TreeMultimap;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.common.io.*;
import dbf0.disk_key_value.readonly.multivalue.InMemoryMultiValueResult;
import dbf0.disk_key_value.readonly.multivalue.KeyMultiValueFileReader;
import dbf0.disk_key_value.readonly.multivalue.KeyMultiValueFileWriter;
import dbf0.test.RandomSeed;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MultiValueSelectorIteratorTest {

  private static final Logger LOGGER = Dbf0Util.getLogger(MultiValueSelectorIteratorTest.class);
  private static final Comparator<String> KEY_COMPARATOR = String::compareTo;
  private static final Comparator<Integer> VALUE_COMPARATOR = Integer::compareTo;

  private static final String KEY_A = "A";
  private static final String KEY_B = "B";
  private static final String KEY_C = "C";
  private static final ValueWrapper<Integer> PUT_1 = put(1);
  private static final ValueWrapper<Integer> PUT_2 = put(2);
  private static final ValueWrapper<Integer> DEL_1 = del(1);


  @Before public void setUp() throws Exception {
    Dbf0Util.enableConsoleLogging(Level.FINER, true);
  }

  @Test public void testEmpty() throws IOException {
    var iter = iterator();
    assertThat(iter.hasNext()).isFalse();
    assertThatThrownBy(iter::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test public void testEmptyValues() throws IOException {
    var iter = iterator(keyMultiValueRank(KEY_A, 0));
    assertThat(iter.hasNext()).isTrue();
    var entry = iter.next();
    assertThat(entry).isNotNull();
    assertThat(entry.getKey()).isEqualTo(KEY_A);
    assertThat(entry.getValue()).isEmpty();
    assertThat(iter.hasNext()).isFalse();
  }

  @Test public void testSingle() throws IOException {
    var iter = iterator(keyMultiValueRank(KEY_A, 0, PUT_1));
    checkSingleValue(iter, KEY_A, PUT_1);
    assertThat(iter.hasNext()).isFalse();
  }

  @Test public void testTwoValues() throws IOException {
    var iter = iterator(keyMultiValueRank(KEY_A, 0, PUT_1, PUT_2));
    assertThat(iter.hasNext()).isTrue();
    var entry = iter.next();
    assertThat(entry).isNotNull();
    assertThat(entry.getKey()).isEqualTo(KEY_A);
    assertThat(entry.getValue())
        .hasSize(2)
        .containsExactly(PUT_1, PUT_2);
    assertThat(iter.hasNext()).isFalse();
  }

  @Test public void testTwoValuesInDifferentIterators() throws IOException {
    var iter = iterator(keyMultiValueRank(KEY_A, 0, PUT_1), keyMultiValueRank(KEY_A, 1, PUT_2));
    assertThat(iter.hasNext()).isTrue();
    var entry = iter.next();
    assertThat(entry).isNotNull();
    assertThat(entry.getKey()).isEqualTo(KEY_A);
    assertThat(entry.getValue())
        .hasSize(2)
        .containsExactly(PUT_1, PUT_2);
    assertThat(iter.hasNext()).isFalse();
  }

  @Test public void testTwoValuesInDifferentIteratorsSwap() throws IOException {
    var iter = iterator(keyMultiValueRank(KEY_A, 0, PUT_2), keyMultiValueRank(KEY_A, 1, PUT_1));
    assertThat(iter.hasNext()).isTrue();
    var entry = iter.next();
    assertThat(entry).isNotNull();
    assertThat(entry.getKey()).isEqualTo(KEY_A);
    assertThat(entry.getValue())
        .hasSize(2)
        .containsExactly(PUT_1, PUT_2);
    assertThat(iter.hasNext()).isFalse();
  }

  @Test public void testSingleDuplicate() throws IOException {
    var iter = iterator(keyMultiValueRank(KEY_A, 0, PUT_1), keyMultiValueRank(KEY_A, 1, PUT_1));
    checkSingleValue(iter, KEY_A, PUT_1);
    assertThat(iter.hasNext()).isFalse();
  }

  @Test public void testSingleDelete() throws IOException {
    var iter = iterator(keyMultiValueRank(KEY_A, 0, DEL_1));
    assertThat(iter.hasNext()).isTrue();
    var entry = iter.next();
    assertThat(entry).isNotNull();
    assertThat(entry.getKey()).isEqualTo(KEY_A);
    assertThat(entry.getValue()).isEmpty();
    assertThat(iter.hasNext()).isFalse();
  }

  @Test public void testDeleteHighRank() throws IOException {
    var iter = iterator(keyMultiValueRank(KEY_A, 1, DEL_1), keyMultiValueRank(KEY_A, 0, PUT_1));
    assertThat(iter.hasNext()).isTrue();
    var entry = iter.next();
    assertThat(entry).isNotNull();
    assertThat(entry.getKey()).isEqualTo(KEY_A);
    assertThat(entry.getValue()).isEmpty();
    assertThat(iter.hasNext()).isFalse();
  }

  @Test public void testDeleteLowRank() throws IOException {
    var iter = iterator(keyMultiValueRank(KEY_A, 0, DEL_1), keyMultiValueRank(KEY_A, 1, PUT_1));
    checkSingleValue(iter, KEY_A, PUT_1);
    assertThat(iter.hasNext()).isFalse();
  }

  @Test public void testTwoKeysSameValue() throws IOException {
    var iter = iterator(keyMultiValueRank(KEY_A, 0, PUT_1), keyMultiValueRank(KEY_B, 0, PUT_1));
    checkSingleValue(iter, KEY_A, PUT_1);
    checkSingleValue(iter, KEY_B, PUT_1);
    assertThat(iter.hasNext()).isFalse();
  }

  @Test public void testTwoKeysDiffValue() throws IOException {
    var iter = iterator(keyMultiValueRank(KEY_A, 0, PUT_1), keyMultiValueRank(KEY_B, 0, PUT_2));
    checkSingleValue(iter, KEY_A, PUT_1);
    checkSingleValue(iter, KEY_B, PUT_2);
    assertThat(iter.hasNext()).isFalse();
  }

  @Test public void testTwoKeysSameValueSwap() throws IOException {
    var iter = iterator(keyMultiValueRank(KEY_A, 0, PUT_1), keyMultiValueRank(KEY_B, 0, PUT_1));
    checkSingleValue(iter, KEY_A, PUT_1);
    checkSingleValue(iter, KEY_B, PUT_1);
    assertThat(iter.hasNext()).isFalse();
  }

  @Test public void testCreateSortedIteratorEmpty() throws IOException {
    var sortedIterator = MultiValueSelectorIterator.createSortedIterator(List.of(), KEY_COMPARATOR);
    assertThat(sortedIterator.hasNext()).isFalse();
    assertThatThrownBy(sortedIterator::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test public void testCreateSortedIteratorSingleEmpty() throws IOException {
    var sortedIterator = MultiValueSelectorIterator.createSortedIterator(
        List.of(createReader(multimap())), KEY_COMPARATOR);
    assertThat(sortedIterator.hasNext()).isFalse();
    assertThatThrownBy(sortedIterator::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test public void testCreateSortedIteratorSingle() throws IOException {
    var multimap = multimap();
    multimap.put(KEY_A, PUT_1);
    var reader = createReader(multimap);
    var sortedIterator = MultiValueSelectorIterator.createSortedIterator(List.of(reader), KEY_COMPARATOR);

    assertThat(sortedIterator.hasNext()).isTrue();
    var group = sortedIterator.next();
    assertThat(group).isNotNull().hasSize(1);
    var entry = group.get(0);
    assertThat(entry.getKey()).isEqualTo(KEY_A);
    assertThat(entry.getRank()).isEqualTo(0);
    assertThat(entry.getValues().realizeRemaining()).hasSize(1).element(0).isEqualTo(PUT_1);

    assertThat(sortedIterator.hasNext()).isFalse();
    reader.close();
  }


  @Test public void testCreateSortedIteratorMultiple() throws IOException {
    var mm1 = multimap();
    mm1.put(KEY_A, PUT_1);
    mm1.put(KEY_A, PUT_2);
    mm1.put(KEY_C, PUT_1);
    var reader1 = createReader(mm1);

    var mm2 = multimap();
    mm2.put(KEY_A, PUT_1);
    mm2.put(KEY_B, PUT_2);
    mm2.put(KEY_C, PUT_1);
    var reader2 = createReader(mm2);

    var sortedIterator = MultiValueSelectorIterator.createSortedIterator(List.of(reader1, reader2), KEY_COMPARATOR);
    assertThat(sortedIterator.hasNext()).isTrue();
    var entries = new ArrayList<KeyRankRealizedValues>();
    while (sortedIterator.hasNext()) {
      for (var entry : sortedIterator.next()) {
        entries.add(KeyRankRealizedValues.of(entry));
      }
    }
    reader1.close();
    reader2.close();

    assertThat(entries)
        .containsExactlyInAnyOrder(
            new KeyRankRealizedValues(KEY_A, 0, List.of(PUT_1, PUT_2)),
            new KeyRankRealizedValues(KEY_C, 0, List.of(PUT_1)),
            new KeyRankRealizedValues(KEY_A, 1, List.of(PUT_1)),
            new KeyRankRealizedValues(KEY_B, 1, List.of(PUT_2)),
            new KeyRankRealizedValues(KEY_C, 1, List.of(PUT_1))
        );

    for (int i = 0; i < entries.size() - 1; i++) {
      assertThat(entries.get(i).key).describedAs(String.valueOf(i)).isLessThanOrEqualTo(entries.get(i + 1).key);
    }
  }

  @Test public void testMergeRandom() throws IOException {
    var random = RandomSeed.CAFE.random();
    var maps = IntStream.range(0, 10).mapToObj(ignored -> randomPutsAndDeletes(random)).collect(Collectors.toList());
    var expected = mergedMultimaps(maps);
    LOGGER.info("Expecting " + expected.keySet().size() + " keys and " + expected.size() + " entries");
    var stores = maps.stream().map(IOFunction.wrap(MultiValueSelectorIteratorTest::createReader))
        .collect(Collectors.toList());

    var results = HashMultimap.<String, ValueWrapper<Integer>>create(expected.asMap().size(), 8);
    var iter = MultiValueSelectorIterator.createSortedAndSelectedIterator(stores, KEY_COMPARATOR, VALUE_COMPARATOR);
    while (iter.hasNext()) {
      var entry = iter.next();
      if (!entry.getValue().isEmpty()) {
        results.putAll(entry.getKey(), entry.getValue());
      }
    }

    assertThat(results.entries()).containsExactlyInAnyOrderElementsOf(expected.entries());
  }

  private static class KeyRankRealizedValues {
    private final String key;
    private final int rank;
    private final List<ValueWrapper<Integer>> values;

    private KeyRankRealizedValues(String key, int rank, List<ValueWrapper<Integer>> values) {
      this.key = key;
      this.rank = rank;
      this.values = values;
    }

    private static KeyRankRealizedValues of(MultiValueSelectorIterator.KeyMultiValueRank<String, Integer> x)
        throws IOException {
      return new KeyRankRealizedValues(x.key, x.rank, x.values.realizeRemaining());
    }

    @Override public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      KeyRankRealizedValues that = (KeyRankRealizedValues) o;

      if (rank != that.rank) return false;
      if (!key.equals(that.key)) return false;
      return values.equals(that.values);
    }

    @Override public int hashCode() {
      int result = key.hashCode();
      result = 31 * result + rank;
      result = 31 * result + values.hashCode();
      return result;
    }

    @Override public String toString() {
      return MoreObjects.toStringHelper("R")
          .add("key", key)
          .add("rank", rank)
          .add("values", values)
          .toString();
    }
  }

  private void checkSingleValue(MultiValueSelectorIterator<String, Integer> iter,
                                String key, ValueWrapper<Integer> value) throws IOException {
    assertThat(iter.hasNext()).isTrue();
    var entry = iter.next();
    assertThat(entry).isNotNull();
    assertThat(entry.getKey()).isEqualTo(key);
    assertThat(entry.getValue()).hasSize(1).element(0).isEqualTo(value);
  }

  private static ValueWrapper<Integer> put(int i) {
    return new ValueWrapper<>(false, i);
  }

  private static ValueWrapper<Integer> del(int i) {
    return new ValueWrapper<>(true, i);
  }

  private static MultiValueSelectorIterator.KeyMultiValueRank<String, Integer> keyMultiValueRank(
      String key, Integer rank, List<ValueWrapper<Integer>> values) {
    return new MultiValueSelectorIterator.KeyMultiValueRank<>(key,
        new InMemoryMultiValueResult<>(values), rank);
  }

  private static MultiValueSelectorIterator.KeyMultiValueRank<String, Integer> keyMultiValueRank(
      String key, Integer rank, ValueWrapper<Integer>... values) {
    return keyMultiValueRank(key, rank, List.of(values));
  }


  private static MultiValueSelectorIterator<String, Integer> iterator(
      List<MultiValueSelectorIterator.KeyMultiValueRank<String, Integer>> values) {
    MergeSortGroupingIOIterator<MultiValueSelectorIterator.KeyMultiValueRank<String, Integer>> grouped =
        new MergeSortGroupingIOIterator<>(
            values.stream().map(x -> IOIterator.of(List.of(x).iterator()))
                .collect(Collectors.toList()),
            Comparator.comparing(MultiValueSelectorIterator.KeyMultiValueRank::getKey));
    return new MultiValueSelectorIterator<>(grouped, VALUE_COMPARATOR);
  }

  @SafeVarargs @SuppressWarnings("varargs")
  private static MultiValueSelectorIterator<String, Integer> iterator(
      MultiValueSelectorIterator.KeyMultiValueRank<String, Integer>... values) {
    return iterator(List.of(values));
  }

  private static TreeMultimap<String, ValueWrapper<Integer>> multimap() {
    return TreeMultimap.create(String::compareTo, ValueWrapper.comparator(Integer::compareTo));
  }

  private static KeyMultiValueFileReader<String, ValueWrapper<Integer>> createReader
      (TreeMultimap<String, ValueWrapper<Integer>> entries) throws IOException {
    var valueSerialization = ValueWrapper.serializationPair(
        new SerializationPair<>(UnsignedIntSerializer.getInstance(), UnsignedIntDeserializer.getInstance()));
    var stream = new ByteArrayOutputStream();
    try (var writer = new KeyMultiValueFileWriter<String, ValueWrapper<Integer>>(
        new StringSerializer(), valueSerialization.getSerializer(), stream)) {
      for (var entry : entries.asMap().entrySet()) {
        writer.writeKeysAndValues(entry.getKey(), entry.getValue());
      }
    }
    return new KeyMultiValueFileReader<>(new StringDeserializer(), valueSerialization.getDeserializer(),
        new ByteArrayInputStream(stream.toByteArray()));
  }

  private static KeyMultiValueFileReader<String, ValueWrapper<Integer>> createReader
      (Multimap<String, ValueWrapper<Integer>> unsorted) throws IOException {
    var sorted = multimap();
    sorted.putAll(unsorted);
    return createReader(sorted);
  }

  private static Multimap<String, ValueWrapper<Integer>> randomPutsAndDeletes(Random random) {
    var result = new PutAndDeletes<String, Integer>(300);
    IntStream.range(0, 50 + random.nextInt(100)).forEach(ignored -> {
      var key = ByteArrayWrapper.random(random, 1).hexString();
      var value = random.nextInt(64);
      if (random.nextFloat() < 0.2) {
        result.delete(key, value);
      } else {
        result.put(key, value);
      }
    });
    return result.getCombined();
  }

  private static Multimap<String, ValueWrapper<Integer>> mergedMultimaps(
      Collection<Multimap<String, ValueWrapper<Integer>>> mms) {
    var merged = HashMultimap.<String, ValueWrapper<Integer>>
        create(mms.stream().mapToInt(Multimap::size).sum(), 8);
    for (var mm : mms) {
      merged.putAll(mm);
    }
    return Multimaps.filterValues(merged, Predicates.not(ValueWrapper::isDelete));
  }
}
