package dbf0.common;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PrefixIoTest {

  private static final long RANDOM_SEED = 0xCAFE;

  private static ByteArrayWrapper bw(int... ints) {
    var bytes = new byte[ints.length];
    for (int i = 0; i < ints.length; i++) {
      bytes[i] = (byte) ints[i];
    }
    return new ByteArrayWrapper(bytes);
  }

  private static ByteArrayWrapper randomBw(int length) {
    var random = new Random(RANDOM_SEED);
    var bytes = new byte[length];
    random.nextBytes(bytes);
    return new ByteArrayWrapper(bytes);
  }

  private static ByteArrayWrapper cat(ByteArrayWrapper... bws) {
    var total = Arrays.stream(bws).mapToInt(bw -> bw.getArray().length).sum();
    var bytes = new byte[total];
    int offset = 0;
    for (ByteArrayWrapper bw : bws) {
      var a = bw.getArray();
      System.arraycopy(a, 0, bytes, offset, a.length);
      offset += a.length;
    }
    assertThat(offset).isEqualTo(total);
    return new ByteArrayWrapper(bytes);
  }

  private static ByteArrayWrapper write(ByteArrayWrapper w) throws IOException {
    var s = new ByteArrayOutputStream();
    PrefixIo.writePrefixLengthBytes(s, w);
    return new ByteArrayWrapper(s.toByteArray());
  }

  private static ByteArrayWrapper read(ByteArrayWrapper w) throws IOException {
    return PrefixIo.readPrefixLengthBytes(new ByteArrayInputStream(w.getArray()));
  }

  private static void roundTripTest(ByteArrayWrapper bw) throws IOException {
    assertThat(read(write(bw))).isEqualTo(bw);
  }

  private static void offsetBwEqual(ByteArrayWrapper shorter, ByteArrayWrapper longer, int offset) {
    assertThat(shorter.getArray().length).isEqualTo(longer.getArray().length - offset);
    assertThat(Arrays.equals(shorter.getArray(), 0, shorter.getArray().length,
        longer.getArray(), offset, longer.getArray().length)).isTrue();
  }

  @Test
  public void testWriteEmpty() throws IOException {
    assertThat(write(bw())).isEqualTo(bw(0));
  }

  @Test
  public void testWriteSingle() throws IOException {
    assertThat(write(bw(5))).isEqualTo(bw(1, 5));
  }

  @Test
  public void testWriteMaxSinglePrefix() throws IOException {
    var bw = randomBw(0xFF - 1);
    var results = write(bw);
    assertThat(results.getArray()).hasSize(0xFF);
    assertThat(results.getArray()[0]).isEqualTo((byte) (0xFF - 1));
    offsetBwEqual(bw, results, 1);
  }

  @Test
  public void testWriteMinTwoPrefix() throws IOException {
    var bw = randomBw(0xFF);
    var results = write(bw);
    assertThat(results.getArray()).hasSize(0xFF + 2);
    assertThat(results.getArray()[0]).isEqualTo((byte) 0xFF);
    assertThat(results.getArray()[1]).isEqualTo((byte) 0);
    offsetBwEqual(bw, results, 2);
  }

  @Test
  public void testWriteMaxTwoPrefix() throws IOException {
    var bw = randomBw(0xFFFF - 1);
    var results = write(bw);
    assertThat(results.getArray()).hasSize(0xFFFF + 1);
    assertThat(results.getArray()[0]).isEqualTo((byte) 0xFF);
    assertThat(results.getArray()[1]).isEqualTo((byte) (0xFF - 1));
    offsetBwEqual(bw, results, 2);
  }

  @Test
  public void testWriteMainThreePrefix() throws IOException {
    var bw = randomBw(0xFFFF);
    var results = write(bw);
    assertThat(results.getArray()).hasSize(0xFFFF + 3);
    assertThat(results.getArray()[0]).isEqualTo((byte) 0xFF);
    assertThat(results.getArray()[1]).isEqualTo((byte) 0xFF);
    assertThat(results.getArray()[2]).isEqualTo((byte) 0);
    offsetBwEqual(bw, results, 3);
  }

  @Test
  public void testReadEmpty() throws IOException {
    assertThat(read(bw(0))).isEqualTo(bw());
  }

  @Test
  public void testReadSingle() throws IOException {
    assertThat(read(bw(1, 9))).isEqualTo(bw(9));
  }

  @Test
  public void testReadMaxSinglePrefix() throws IOException {
    var bw = randomBw(0xFF - 1);
    assertThat(read(cat(bw(0xFF - 1), bw))).isEqualTo(bw);
  }

  @Test
  public void testReadMinTwoPrefix() throws IOException {
    var bw = randomBw(0xFF);
    assertThat(read(cat(bw(0xFF, 0), bw))).isEqualTo(bw);
  }

  @Test
  public void testReadMaxTwoPrefix() throws IOException {
    var bw = randomBw(0xFFFF - 1);
    assertThat(read(cat(bw(0xFF, 0xFF - 1), bw))).isEqualTo(bw);
  }

  @Test
  public void testReadMinThreePrefix() throws IOException {
    var bw = randomBw(0xFFFF);
    assertThat(read(cat(bw(0xFF, 0xFF, 0), bw))).isEqualTo(bw);
  }

  @Test
  public void testReadEmptyStream() throws IOException {
    assertThatThrownBy(() -> read(bw()))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Unexpected end of input stream");
  }

  @Test
  public void testRoundTripEmpty() throws IOException {
    roundTripTest(bw());
  }

  @Test
  public void testRoundTripSingle() throws IOException {
    roundTripTest(bw(1));
    roundTripTest(bw(5));
    roundTripTest(bw(99));
  }

  @Test
  public void testRoundMaxSinglePrefix() throws IOException {
    roundTripTest(randomBw(0xFF - 1));
  }

  @Test
  public void testRoundMinTwoPrefix() throws IOException {
    roundTripTest(randomBw(0xFF));
  }

  @Test
  public void testRoundMaxTwoPrefix() throws IOException {
    roundTripTest(randomBw(0xFFFF - 1));
  }

  @Test
  public void testRoundMinThreePrefix() throws IOException {
    roundTripTest(randomBw(0xFFFF));
  }
}
