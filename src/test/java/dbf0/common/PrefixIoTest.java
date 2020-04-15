package dbf0.common;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;

import static dbf0.common.ByteArrayWrapper.cat;
import static dbf0.common.PrefixIo.END_OF_LENGTH;
import static dbf0.common.PrefixIo.ESCAPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PrefixIoTest {

  private static final long RANDOM_SEED = 0xCAFE;

  private static ByteArrayWrapper writeLength(int length) throws IOException {
    var s = new ByteArrayOutputStream();
    PrefixIo.writeLength(s, length);
    return ByteArrayWrapper.of(s.toByteArray());
  }

  private static int readLength(ByteArrayWrapper bw) throws IOException {
    return PrefixIo.readLength((new ByteArrayInputStream(bw.getArray())));
  }

  private static ByteArrayWrapper bw(int... ints) {
    return ByteArrayWrapper.of(ints);
  }

  private static ByteArrayWrapper randomBw(int length) {
    var random = random();
    var bytes = new byte[length];
    random.nextBytes(bytes);
    return new ByteArrayWrapper(bytes);
  }

  private static ByteArrayWrapper write(ByteArrayWrapper w) throws IOException {
    var s = new ByteArrayOutputStream();
    PrefixIo.writeBytes(s, w);
    return new ByteArrayWrapper(s.toByteArray());
  }

  private static ByteArrayWrapper read(ByteArrayWrapper w) throws IOException {
    return PrefixIo.readBytes(new ByteArrayInputStream(w.getArray()));
  }

  private static void roundTripTest(ByteArrayWrapper bw) throws IOException {
    assertThat(read(write(bw))).isEqualTo(bw);
  }

  private static void offsetBwEqual(ByteArrayWrapper shorter, ByteArrayWrapper longer, int offset) {
    assertThat(shorter.getArray().length).isEqualTo(longer.getArray().length - offset);
    assertThat(Arrays.equals(shorter.getArray(), 0, shorter.getArray().length,
        longer.getArray(), offset, longer.getArray().length)).isTrue();
  }

  private static Random random() {
    return new Random(RANDOM_SEED);
  }

  @Test
  public void testWriteZeroLength() throws IOException {
    assertThat(writeLength(0)).isEqualTo(bw(END_OF_LENGTH));
  }

  @Test
  public void testWriteSingleLength() throws IOException {
    assertThat(writeLength(5)).isEqualTo(bw(5, END_OF_LENGTH));
  }

  @Test
  public void testWriteEolLength() throws IOException {
    assertThat(writeLength(END_OF_LENGTH)).isEqualTo(bw(ESCAPE, END_OF_LENGTH, END_OF_LENGTH));
  }

  @Test
  public void testWriteEscapeLength() throws IOException {
    assertThat(writeLength(ESCAPE)).isEqualTo(bw(ESCAPE, ESCAPE, END_OF_LENGTH));
  }

  @Test
  public void testWriteTwoLength() throws IOException {
    assertThat(writeLength(0x100)).isEqualTo(bw(0, 1, END_OF_LENGTH));
  }

  @Test
  public void testReadZeroLength() throws IOException {
    assertThat(readLength(bw(END_OF_LENGTH))).isEqualTo(0);
  }

  @Test
  public void testReadSingleLength() throws IOException {
    assertThat(readLength(bw(12, END_OF_LENGTH))).isEqualTo(12);
  }

  @Test
  public void testReadEolLength() throws IOException {
    assertThat(readLength(bw(ESCAPE, END_OF_LENGTH, END_OF_LENGTH))).isEqualTo(END_OF_LENGTH);
  }

  @Test
  public void testReadEscapeLength() throws IOException {
    assertThat(readLength(bw(ESCAPE, ESCAPE, END_OF_LENGTH))).isEqualTo(ESCAPE);
  }

  @Test
  public void testReadTwoLength() throws IOException {
    assertThat(readLength(bw(0, 1, END_OF_LENGTH))).isEqualTo(0x100);
  }

  @Test
  public void testLengthSingleRoundTrip() {
    IntStream.range(0, 0xFF).boxed().forEach(IoConsumer.wrap(
        i -> assertThat(readLength(writeLength(i))).isEqualTo(i)
    ));
  }

  @Test
  public void testLengthTwoRoundTrip() {
    var random = random();
    IntStream.range(0, 10).boxed().forEach(IoConsumer.wrap(i -> {
      var length = random.nextInt(0xFF00) + 0xFF;
      assertThat(readLength(writeLength(length))).isEqualTo(length);
    }));
  }

  @Test
  public void testWriteEmpty() throws IOException {
    assertThat(write(bw())).isEqualTo(bw(END_OF_LENGTH));
  }

  @Test
  public void testWriteSingle() throws IOException {
    assertThat(write(bw(5))).isEqualTo(bw(1, END_OF_LENGTH, 5));
  }

  @Test
  public void testWriteMaxSinglePrefix() throws IOException {
    var bw = randomBw(0xFF - 1);
    var results = write(bw);
    assertThat(results.getArray()).hasSize(0xFF + 1);
    assertThat(results.getArray()[0]).isEqualTo((byte) (0xFF - 1));
    assertThat(results.getArray()[1]).isEqualTo((byte) END_OF_LENGTH);
    offsetBwEqual(bw, results, 2);
  }

  @Test
  public void testWriteMinTwoPrefix() throws IOException {
    var bw = randomBw(0xFF);
    var results = write(bw);
    assertThat(results.getArray()).hasSize(0xFF + 2);
    assertThat(results.getArray()[0]).isEqualTo((byte) 0xFF);
    assertThat(results.getArray()[1]).isEqualTo((byte) END_OF_LENGTH);
    offsetBwEqual(bw, results, 2);
  }

  @Test
  public void testWriteMaxTwoPrefix() throws IOException {
    var bw = randomBw(0xFFFF - 1);
    var results = write(bw);
    assertThat(results.getArray()).hasSize(0xFFFF + 2);
    assertThat(results.getArray()[0]).isEqualTo((byte) (0xFF - 1));
    assertThat(results.getArray()[1]).isEqualTo((byte) 0xFF);
    assertThat(results.getArray()[2]).isEqualTo((byte) END_OF_LENGTH);
    offsetBwEqual(bw, results, 3);
  }

  @Test
  public void testWriteMainThreePrefix() throws IOException {
    var bw = randomBw(0xFFFF);
    var results = write(bw);
    assertThat(results.getArray()).hasSize(0xFFFF + 3);
    assertThat(results.getArray()[0]).isEqualTo((byte) 0xFF);
    assertThat(results.getArray()[1]).isEqualTo((byte) 0xFF);
    assertThat(results.getArray()[2]).isEqualTo((byte) END_OF_LENGTH);
    offsetBwEqual(bw, results, 3);
  }

  @Test
  public void testReadEmpty() throws IOException {
    assertThat(read(bw(END_OF_LENGTH))).isEqualTo(bw());
  }

  @Test
  public void testReadSingle() throws IOException {
    assertThat(read(bw(1, END_OF_LENGTH, 9))).isEqualTo(bw(9));
  }

  @Test
  public void testReadMaxSinglePrefix() throws IOException {
    var bw = randomBw(0xFF - 1);
    assertThat(read(cat(bw(0xFF - 1, END_OF_LENGTH), bw))).isEqualTo(bw);
  }

  @Test
  public void testReadMinTwoPrefix() throws IOException {
    var bw = randomBw(0xFF);
    assertThat(read(cat(bw(0xFF, END_OF_LENGTH), bw))).isEqualTo(bw);
  }

  @Test
  public void testReadMaxTwoPrefix() throws IOException {
    var bw = randomBw(0xFFFF - 1);
    assertThat(read(cat(bw(0xFF - 1, 0xFF, END_OF_LENGTH), bw))).isEqualTo(bw);
  }

  @Test
  public void testReadEmptyStream() {
    assertThatThrownBy(() -> read(bw()))
        .isInstanceOf(EndOfStream.class)
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
  public void testReadMinThreePrefix() throws IOException {
    var bw = randomBw(0xFFFF);
    assertThat(read(cat(bw(0xFF, 0xFF, END_OF_LENGTH), bw))).isEqualTo(bw);
  }

  @Test
  public void testRoundTripTwoPrefix() {
    var random = random();
    IntStream.range(0, 10).boxed().forEach(IoConsumer.wrap(i -> {
      var bw = randomBw(random.nextInt(0xFF00) + 0xFF);
      var w = write(bw);
      assertThat(w.getArray()).hasSize(bw.getArray().length + 3);
      var r = read(w);
      assertThat(r).isEqualTo(bw);
    }));
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
