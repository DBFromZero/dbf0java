package dbf0.common.io;

import dbf0.common.ByteArrayWrapper;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.stream.IntStream;

import static dbf0.common.io.IOUtil.EIGHTH_BIT_SET;
import static dbf0.common.io.IOUtil.LOWER_7BITS_SET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class IOUtilTest {

  private static final long RANDOM_SEED = 0xCAFE;

  private static ByteArrayWrapper writeInt(int length) throws IOException {
    var s = new ByteArrayOutputStream();
    IOUtil.writeVariableLengthUnsignedInt(s, length);
    return ByteArrayWrapper.of(s.toByteArray());
  }

  private static int readInt(ByteArrayWrapper bw) throws IOException {
    return IOUtil.readVariableLengthUnsignedInt((new ByteArrayInputStream(bw.getArray())));
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
    IOUtil.writeBytes(s, w);
    return new ByteArrayWrapper(s.toByteArray());
  }

  private static ByteArrayWrapper read(ByteArrayWrapper w) throws IOException {
    return IOUtil.readBytes(new ByteArrayInputStream(w.getArray()));
  }

  private static void roundTripTest(ByteArrayWrapper bw) throws IOException {
    assertThat(read(write(bw))).isEqualTo(bw);
  }

  private static Random random() {
    return new Random(RANDOM_SEED);
  }

  @Test public void testWriteZero() throws IOException {
    assertThat(writeInt(0)).isEqualTo(bw(0));
  }

  @Test public void testWriteSingleByte() throws IOException {
    assertThat(writeInt(5)).isEqualTo(bw(5));
  }

  @Test public void testWriteEightByte() throws IOException {
    assertThat(writeInt(EIGHTH_BIT_SET)).isEqualTo(bw(EIGHTH_BIT_SET, 1));
  }

  @Test public void testWriteEightOnes() throws IOException {
    assertThat(writeInt(0xFF)).isEqualTo(bw(0xFF, 1));
  }

  @Test public void testWriteTwoBytes() throws IOException {
    assertThat(writeInt(0x100)).isEqualTo(bw(EIGHTH_BIT_SET, 2));
  }

  @Test public void testReadZero() throws IOException {
    assertThat(readInt(bw(0))).isEqualTo(0);
  }

  @Test public void testReadSingleByte() throws IOException {
    assertThat(readInt(bw(12))).isEqualTo(12);
  }

  @Test public void testReadEigthByte() throws IOException {
    assertThat(readInt(bw(EIGHTH_BIT_SET, 1))).isEqualTo(EIGHTH_BIT_SET);
  }

  @Test public void testReadEightOnes() throws IOException {
    assertThat(readInt(bw(0xFF, 1))).isEqualTo(0xFF);
  }

  @Test public void testReadTwoLength() throws IOException {
    assertThat(readInt(bw(EIGHTH_BIT_SET, 2))).isEqualTo(0x100);
  }

  @Test public void testSingleRoundTrip() {
    IntStream.range(0, LOWER_7BITS_SET).boxed().forEach(IOConsumer.wrap(
        i -> assertThat(readInt(writeInt(i))).isEqualTo(i)
    ));
  }

  @Test public void testTwoRoundTrip() {
    var random = random();
    IntStream.range(0, 10).boxed().forEach(IOConsumer.wrap(i -> {
      var x = random.nextInt(LOWER_7BITS_SET << 7) + LOWER_7BITS_SET;
      assertThat(readInt(writeInt(x))).isEqualTo(x);
    }));
  }

  @Test public void testLargeIntRoundTrip() {
    var random = random();
    IntStream.range(0, 10).boxed().forEach(IOConsumer.wrap(i -> {
      var x = Math.abs(random.nextInt());
      assertThat(readInt(writeInt(x))).isEqualTo(x);
    }));
  }

  @Test public void testLongRoundTrip() {
    var random = random();
    IntStream.range(0, 10).boxed().forEach(IOConsumer.wrap(i -> {
      var x = Math.abs(random.nextLong());
      var s = new ByteArrayOutputStream();
      IOUtil.writeVariableLengthUnsignedLong(s, x);
      var r = IOUtil.readVariableLengthUnsignedLong(new ByteArrayInputStream(s.toByteArray()));
      assertThat(r).isEqualTo(x);
    }));
  }

  @Test public void testIntReadEmptyStream() {
    assertThatThrownBy(() -> readInt(bw())).isInstanceOf(EndOfStream.class);
  }

  @Test public void testIntReadInsufficientInputStream() throws IOException {
    var full = writeInt(13242343);
    var partial = full.slice(0, full.length() - 1);
    assertThatThrownBy(() -> readInt(partial)).isInstanceOf(EndOfStream.class);
  }

  @Test public void testWriteEmpty() throws IOException {
    assertThat(write(bw())).isEqualTo(bw(0));
  }

  @Test public void testWriteSingle() throws IOException {
    assertThat(write(bw(5))).isEqualTo(bw(1, 5));
  }

  @Test public void testReadEmptyStream() {
    assertThatThrownBy(() -> read(bw())).isInstanceOf(EndOfStream.class);
  }

  @Test public void testReadInsufficientInputStream() throws IOException {
    var full = write(bw(1));
    var partial = full.slice(0, full.length() - 1);
    assertThatThrownBy(() -> read(partial)).isInstanceOf(EndOfStream.class);
  }

  @Test public void testRoundTripEmpty() throws IOException {
    roundTripTest(bw());
  }

  @Test public void testRoundTripSingle() throws IOException {
    roundTripTest(bw(1));
    roundTripTest(bw(5));
    roundTripTest(bw(99));
  }

  @Test public void testRoundMaxSinglePrefix() throws IOException {
    roundTripTest(randomBw(LOWER_7BITS_SET - 1));
  }

  @Test public void testRoundMinTwoPrefix() throws IOException {
    roundTripTest(randomBw(EIGHTH_BIT_SET));
  }

  @Test public void testRoundTripTwoPrefix() {
    var random = random();
    IntStream.range(0, 10).boxed().forEach(IOConsumer.wrap(i -> {
      var bw = randomBw(random.nextInt(0xFF00) + 0xFF);
      var w = write(bw);
      var r = read(w);
      assertThat(r).isEqualTo(bw);
    }));
  }

  @Test public void testRoundMaxTwoPrefix() throws IOException {
    roundTripTest(randomBw(0xFFFF - 1));
  }

  @Test public void testRoundMinThreePrefix() throws IOException {
    roundTripTest(randomBw(0xFFFF));
  }
}
