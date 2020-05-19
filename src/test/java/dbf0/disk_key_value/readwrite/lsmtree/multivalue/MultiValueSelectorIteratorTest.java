package dbf0.disk_key_value.readwrite.lsmtree.multivalue;

import com.google.common.collect.TreeMultimap;
import dbf0.common.Dbf0Util;
import dbf0.common.io.*;
import dbf0.disk_key_value.readonly.multivalue.InMemoryMultiValueResult;
import dbf0.disk_key_value.readonly.multivalue.KeyMultiValueFileReader;
import dbf0.disk_key_value.readonly.multivalue.KeyMultiValueFileWriter;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MultiValueSelectorIteratorTest {

  private static final Logger LOGGER = Dbf0Util.getLogger(MultiValueSelectorIteratorTest.class);
  private static final Comparator<String> KEY_COMPARATOR = String::compareTo;
  private static final Comparator<Integer> VALUE_COMPARATOR = Integer::compareTo;

  private static final String KEY_A = "A";
  private static final String KEY_B = "B";
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
    var entry = sortedIterator.next();
    assertThat(entry).isNotNull();
    assertThat(entry.getKey()).isEqualTo(KEY_A);
    assertThat(entry.getRank()).isEqualTo(0);
    assertThat(entry.getValues().realizeRemaining()).hasSize(1).element(0).isEqualTo(PUT_1);

    assertThat(sortedIterator.hasNext()).isFalse();
    reader.close();
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
    return new MultiValueSelectorIterator<>(IOIterator.of(values.iterator()), VALUE_COMPARATOR);
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
}
