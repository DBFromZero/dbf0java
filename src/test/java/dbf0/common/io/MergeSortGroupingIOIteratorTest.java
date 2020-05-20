package dbf0.common.io;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.test.RandomSeed;
import org.apache.commons.lang3.tuple.Pair;
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

public class MergeSortGroupingIOIteratorTest {

  private static final Logger LOGGER = Dbf0Util.getLogger(MergeSortGroupingIOIteratorTest.class);

  @Before public void setUp() throws Exception {
    Dbf0Util.enableConsoleLogging(Level.FINER, true);
  }

  @Test public void testEmpty() throws IOException {
    var iterator = iterator(List.of());
    assertThat(iterator.hasNext()).isFalse();
    assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test public void testSingle() throws IOException {
    var p = p("A", 1);
    var iterator = iterator(List.of(p));
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(List.of(p));
    assertThat(iterator.hasNext()).isFalse();
  }

  @Test public void testTwoKeys() throws IOException {
    var a = p("A", 1);
    var b = p("B", 1);
    var iterator = iterator(List.of(a, b));
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(List.of(a));
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(List.of(b));
    assertThat(iterator.hasNext()).isFalse();
  }

  @Test public void testPairOfTwo() throws IOException {
    var a = p("A", 1);
    var b = p("A", 1);
    var iterator = iterator(List.of(a), List.of(b));
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).containsExactlyInAnyOrderElementsOf(List.of(a, b));
    assertThat(iterator.hasNext()).isFalse();
  }

  @Test public void testComplexRandom() throws IOException {
    var random = RandomSeed.CAFE.random();
    var maps = IntStream.range(0, 10).mapToObj(ignored -> randomMap(random)).collect(Collectors.toList());
    var expected = combineMaps(maps);

    var iterator = iterator(maps.stream().map(MergeSortGroupingIOIteratorTest::sortMapEntry)
        .collect(Collectors.toList()));
    var results = ArrayListMultimap.<String, Integer>create();
    while (iterator.hasNext()) {
      var entry = iterator.next();
      assertThat(entry).isNotEmpty();
      var key = entry.get(0).getKey();
      assertThat(results.keySet()).doesNotContain(key);
      for (var pair : entry) {
        assertThat(pair.getKey()).isEqualTo(key);
        results.put(key, pair.getValue());
      }
    }

    assertThat(results.keySet()).containsExactlyInAnyOrderElementsOf(expected.keySet());
    for (var entry : expected.asMap().entrySet().stream().sorted(Map.Entry.comparingByKey())
        .collect(Collectors.toList())) {
      LOGGER.fine(() -> entry.getKey() + " = " + entry.getValue());
      assertThat(results.get(entry.getKey())).describedAs(entry.getKey())
          .containsExactlyInAnyOrderElementsOf(entry.getValue());
    }
  }

  private static MergeSortGroupingIOIterator<Pair<String, Integer>>
  iterator(List<List<Pair<String, Integer>>> listOfLists) {
    return new MergeSortGroupingIOIterator<>(
        listOfLists.stream().map(List::iterator).map(IOIterator::of).collect(Collectors.toList()),
        Map.Entry.comparingByKey()
    );
  }

  private static MergeSortGroupingIOIterator<Pair<String, Integer>>
  iterator(List<Pair<String, Integer>>... list) {
    return iterator(List.of(list));
  }

  private static Pair<String, Integer> p(String s, Integer i) {
    return Pair.of(s, i);
  }

  private static Map<String, Integer> randomMap(Random r) {
    var map = new HashMap<String, Integer>(128);
    IntStream.range(0, 20 + r.nextInt(120)).forEach(ignored ->
        map.put(ByteArrayWrapper.random(r, 1).hexString(), r.nextInt(1000)));
    return map;
  }

  private static ListMultimap<String, Integer> combineMaps(Collection<Map<String, Integer>> maps) {
    var lm = ArrayListMultimap.<String, Integer>create();
    for (var map : maps) {
      for (var entry : map.entrySet()) {
        lm.put(entry.getKey(), entry.getValue());
      }
    }
    return lm;
  }

  private static List<Pair<String, Integer>> sortMapEntry(Map<String, Integer> map) {
    return map.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .map(e -> Pair.of(e.getKey(), e.getValue()))
        .collect(Collectors.toList());
  }
}
