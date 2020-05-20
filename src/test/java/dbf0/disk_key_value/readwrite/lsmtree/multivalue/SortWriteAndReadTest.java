package dbf0.disk_key_value.readwrite.lsmtree.multivalue;

import dbf0.common.Dbf0Util;
import dbf0.common.io.PositionTrackingStream;
import dbf0.disk_key_value.io.MemoryFileOperations;
import dbf0.disk_key_value.readonly.multivalue.KeyMultiValueFileReader;
import dbf0.disk_key_value.readonly.multivalue.RandomAccessKeyMultiValueFileReader;
import dbf0.test.RandomSeed;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static dbf0.disk_key_value.readwrite.lsmtree.multivalue.MVLUtil.*;
import static org.assertj.core.api.Assertions.assertThat;

public class SortWriteAndReadTest {

  private static final Logger LOGGER = Dbf0Util.getLogger(SortWriteAndReadTest.class);

  private static final SortAndWriteKeyMultiValues<String, Integer> JOB =
      new SortAndWriteKeyMultiValues<>(CONFIGURATION, Integer::compareTo);


  @Before public void setUp() {
    Dbf0Util.enableConsoleLogging(Level.FINER, true);
  }

  @Test public void testEmpty() throws IOException {
    var results = sortWriteAndRead(new PutAndDeletes<>(1));
    assertThat(results).isEmpty();
    sanityCheckResults(results);
  }

  @Test public void testSinglePut() throws IOException {
    var putAndDeletes = new PutAndDeletes<String, Integer>(1);
    putAndDeletes.put(KEY_A, PUT_1.getValue());
    var results = sortWriteAndRead(putAndDeletes);
    assertThat(results).hasSize(1);
    sanityCheckResults(results);
    var entry = results.get(0);
    assertThat(entry.getKey()).isEqualTo(KEY_A);
    assertThat(entry.getValue()).containsExactly(PUT_1);
  }

  @Test public void testTwoKeyPut() throws IOException {
    var putAndDeletes = new PutAndDeletes<String, Integer>(1);
    putAndDeletes.put(KEY_A, PUT_1.getValue());
    putAndDeletes.put(KEY_B, PUT_2.getValue());

    var results = sortWriteAndRead(putAndDeletes);
    assertThat(results).hasSize(2);
    sanityCheckResults(results);

    var first = results.get(0);
    assertThat(first.getKey()).isEqualTo(KEY_A);
    assertThat(first.getValue()).containsExactly(PUT_1);

    var second = results.get(1);
    assertThat(second.getKey()).isEqualTo(KEY_B);
    assertThat(second.getValue()).containsExactly(PUT_2);
  }

  @Test public void testSingleKeyPutTwoValues() throws IOException {
    var putAndDeletes = new PutAndDeletes<String, Integer>(1);
    putAndDeletes.put(KEY_A, PUT_1.getValue());
    putAndDeletes.put(KEY_A, PUT_2.getValue());

    var results = sortWriteAndRead(putAndDeletes);
    assertThat(results).hasSize(1);
    sanityCheckResults(results);

    var entry = results.get(0);
    assertThat(entry.getKey()).isEqualTo(KEY_A);
    assertThat(entry.getValue()).containsExactly(PUT_1, PUT_2);
  }

  @Test public void testSingleDelete() throws IOException {
    var putAndDeletes = new PutAndDeletes<String, Integer>(1);
    putAndDeletes.delete(KEY_A, DEL_1.getValue());
    var results = sortWriteAndRead(putAndDeletes);
    assertThat(results).hasSize(1);
    sanityCheckResults(results);
    var entry = results.get(0);
    assertThat(entry.getKey()).isEqualTo(KEY_A);
    assertThat(entry.getValue()).containsExactly(DEL_1);
  }

  @Test public void testPutThenDelete() throws IOException {
    var putAndDeletes = new PutAndDeletes<String, Integer>(1);
    putAndDeletes.put(KEY_A, DEL_1.getValue());
    putAndDeletes.delete(KEY_A, DEL_1.getValue());
    var results = sortWriteAndRead(putAndDeletes);
    assertThat(results).hasSize(1);
    sanityCheckResults(results);
    var entry = results.get(0);
    assertThat(entry.getKey()).isEqualTo(KEY_A);
    assertThat(entry.getValue()).containsExactly(DEL_1);
  }

  @Test public void testDeleteThenPut() throws IOException {
    var putAndDeletes = new PutAndDeletes<String, Integer>(1);
    putAndDeletes.delete(KEY_A, PUT_1.getValue());
    putAndDeletes.put(KEY_A, PUT_1.getValue());
    var results = sortWriteAndRead(putAndDeletes);
    assertThat(results).hasSize(1);
    sanityCheckResults(results);
    var entry = results.get(0);
    assertThat(entry.getKey()).isEqualTo(KEY_A);
    assertThat(entry.getValue()).containsExactly(PUT_1);
  }

  @Test public void testRandomPutAndDeletes() throws IOException {
    var putAndDeletes = randomPutAndDeletes(RandomSeed.CAFE.random());
    var expected = computeExpected(putAndDeletes);
    var results = sortWriteAndRead(putAndDeletes);
    sanityCheckResults(results);
    assertThat(results).containsExactlyElementsOf(expected);
  }

  private void sanityCheckResults(List<Pair<String, List<ValueWrapper<Integer>>>> results) {
    assertThat(results).isSortedAccordingTo(Map.Entry.comparingByKey());
    results.forEach(pair -> assertThat(pair.getValue()).isSortedAccordingTo(ValueWrapper.comparator(VALUE_COMPARATOR)));
  }

  private List<Pair<String, List<ValueWrapper<Integer>>>> sortWriteAndRead(PutAndDeletes<String, Integer> putAndDeletes)
      throws IOException {
    return read(open(sortAndWrite(putAndDeletes).getLeft()));
  }

  private Pair<MemoryFileOperations, MemoryFileOperations> sortAndWrite(PutAndDeletes<String, Integer> putAndDeletes)
      throws IOException {
    var dataOps = new MemoryFileOperations("data");
    var indexOps = new MemoryFileOperations("index");
    try (var dataStream = new PositionTrackingStream(dataOps.createAppendOutputStream())) {
      try (var indexStream = new BufferedOutputStream(indexOps.createAppendOutputStream())) {
        JOB.sortAndWrite(dataStream, indexStream, putAndDeletes, false);
      }
    }
    return Pair.of(dataOps, indexOps);

  }

  private KeyMultiValueFileReader<String, ValueWrapper<Integer>> open(MemoryFileOperations dataOps)
      throws IOException {
    return new KeyMultiValueFileReader<>(
        CONFIGURATION.getKeySerialization().getDeserializer(),
        CONFIGURATION.getValueSerialization().getDeserializer(),
        dataOps.createInputStream()
    );
  }

  private List<Pair<String, List<ValueWrapper<Integer>>>> read(KeyMultiValueFileReader<String, ValueWrapper<Integer>> reader)
      throws IOException {
    var pairs = new ArrayList<Pair<String, List<ValueWrapper<Integer>>>>(128);
    while (true) {
      var key = reader.readKey();
      if (key == null) {
        break;
      }
      var values = new ArrayList<ValueWrapper<Integer>>(reader.getValuesRemaining());
      reader.valueIterator().forEachRemaining(values::add);
      pairs.add(Pair.of(key, values));
    }
    return pairs;
  }

  private RandomAccessKeyMultiValueFileReader<String, ValueWrapper<Integer>> openRandomAccess(
      MemoryFileOperations dataOps, MemoryFileOperations indexOps) throws IOException {
    return RandomAccessKeyMultiValueFileReader.open(
        CONFIGURATION.getKeySerialization().getDeserializer(),
        CONFIGURATION.getValueSerialization().getDeserializer(),
        CONFIGURATION.getKeyComparator(),
        dataOps, indexOps
    );
  }

  private List<Pair<String, List<ValueWrapper<Integer>>>> computeExpected(PutAndDeletes<String, Integer> putAndDeletes) {
    return putAndDeletes.getCombined().asMap().entrySet().stream()
        .map(p -> Pair.of(p.getKey(), p.getValue().stream().sorted(ValueWrapper.comparator(VALUE_COMPARATOR))
            .collect(Collectors.toList())))
        .sorted(Map.Entry.comparingByKey())
        .collect(Collectors.toList());
  }
}
