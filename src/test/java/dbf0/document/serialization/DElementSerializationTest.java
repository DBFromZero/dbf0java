package dbf0.document.serialization;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.Dbf0Util;
import dbf0.document.types.*;
import dbf0.test.RandomSeed;
import org.apache.commons.codec.binary.Hex;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Random;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class DElementSerializationTest {
  private static final Logger LOGGER = Dbf0Util.getLogger(DElementSerializationTest.class);

  private final DElementSerializer serializer = DElementSerializer.defaultCharsetInstance();
  private final DElementDeserializer deserializer = DElementDeserializer.defaultCharsetInstance();

  @Before public void setUp() throws Exception {
    Dbf0Util.enableConsoleLogging(Level.FINER, true);
  }


  @Test public void testNull() throws IOException {
    testRoundTrip(DNull.getInstance(), 1);
  }

  @Test public void testTrue() throws IOException {
    testRoundTrip(DBool.getTrue(), 1);
  }

  @Test public void testFalse() throws IOException {
    testRoundTrip(DBool.getFalse(), 1);
  }

  @Test public void testIntZero() throws IOException {
    testRoundTrip(DInt.of(0L), 1);
  }

  @Test public void testIntSmall() throws IOException {
    testRoundTrip(DInt.of(3L), 1);
  }

  @Test public void testInt14() throws IOException {
    testRoundTrip(DInt.of(14L), 1);
  }

  @Test public void testInt15() throws IOException {
    testRoundTrip(DInt.of(15L), 2);
  }

  @Test public void testIntMid() throws IOException {
    testRoundTrip(DInt.of(547L));
  }

  @Test public void testIntLarge() throws IOException {
    testRoundTrip(DInt.of(35695382269897L));
  }

  @Test public void testStringEmpty() throws IOException {
    testRoundTrip(DString.of(""), 1);
  }

  @Test public void testStringSingle() throws IOException {
    testRoundTrip(DString.of("a"), 2);
  }

  @Test public void testUnicodeZero() throws IOException {
    testRoundTrip(DString.of(String.valueOf((char) 0)));
  }

  @Test public void testUnicode1000() throws IOException {
    testRoundTrip(DString.of(String.valueOf((char) 1000)));
  }

  @Test public void testUnicodeMax() throws IOException {
    testRoundTrip(DString.of(String.valueOf((char) 65534)));
  }

  @Test public void testStringMid() throws IOException {
    testRoundTrip(DString.of("hI5liFv5Nh2HvPJH"));
  }

  @Test public void testStringAllCharacters() throws IOException {
    testRoundTrip(DString.of(Joiner.on("").join(IntStream.range(0, 50000)
        .filter(i -> i != 55296)
        .mapToObj(i -> (char) i).iterator())));
  }

  @Test public void testDecimalZero() throws IOException {
    testRoundTrip(DDecimal.of(new BigDecimal(0L)));
  }

  @Test public void testDecimalPositive() throws IOException {
    testRoundTrip(DDecimal.of(new BigDecimal("99.34901097702962")));
  }

  @Test public void testDecimalNegative() throws IOException {
    testRoundTrip(DDecimal.of(new BigDecimal("-407.6913205215268")));
  }

  @Test public void testArrayEmpty() throws IOException {
    testRoundTrip(DArray.of(), 1);
  }

  @Test public void testArraySingle() throws IOException {
    testRoundTrip(DArray.of(DInt.of(1)), 2);
  }

  @Test public void testArrayLarge() throws IOException {
    testRoundTrip(DArray.of(IntStream.range(0, 10000).mapToObj(DInt::new).collect(ImmutableList.toImmutableList())));
  }

  @Test public void testMapEmpty() throws IOException {
    testRoundTrip(DMap.of(), 1);
  }

  @Test public void testMapSingle() throws IOException {
    testRoundTrip(DMap.of(DInt.of(1), DInt.of(2)), 3);
  }

  @Test public void testMapLarge() throws IOException {
    testRoundTrip(DMap.of(IntStream.range(0, 10000).boxed().collect(ImmutableMap.toImmutableMap(
        (Function<Integer, DElement>) DInt::of,
        i -> DString.of(String.valueOf(i))
    ))));
  }

  @Test public void testComplex() throws IOException {
    testRoundTrip(randomElement(RandomSeed.CAFE.random(), 3));
  }


  private DElement randomElement(Random random, int depthRemaining) {
    Preconditions.checkArgument(depthRemaining >= 0);
    var types = DElementType.values();
    var type = types[random.nextInt(types.length)];
    if (depthRemaining == 0 && (type == DElementType.ARRAY || type == DElementType.MAP)) {
      type = DElementType.NULL;
    }
    switch (type) {
      case NULL:
        return DNull.getInstance();
      case BOOL:
        return DBool.of(random.nextBoolean());
      case INT:
        return DInt.of(random.nextLong());
      case DECIMAL:
        return DDecimal.of(BigDecimal.valueOf(random.nextDouble()));
      case STRING:
        return DString.of(Hex.encodeHexString(ByteArrayWrapper.random(random, random.nextInt(100)).getArray()));
      case ARRAY:
        return DArray.of(IntStream.range(0, random.nextInt(10))
            .mapToObj(ignored -> randomElement(random, depthRemaining - 1))
            .collect(ImmutableList.toImmutableList()));
      case MAP:
        return randomMap(random, depthRemaining);
      default:
        throw new RuntimeException("Unhandled type " + type);
    }
  }

  private DMap randomMap(Random random, int depthRemaining) {
    var d = depthRemaining - 1;
    return DMap.of(IntStream.range(0, random.nextInt(10)).boxed().collect(Collectors.toMap(
        ignored -> randomElement(random, d),
        ignored -> randomElement(random, d),
        (a, b) -> DArray.of(a, b)
    )));
  }

  private void testRoundTrip(DElement original) throws IOException {
    testRoundTrip(original, null);
  }

  private void testRoundTrip(DElement original, @Nullable Integer expectedSize) throws IOException {
    var bytes = serializer.serializeToBytes(original);
    LOGGER.finer(() -> String.format("%s serializes to %s", original, bytes));
    if (expectedSize != null) {
      assertThat(bytes.getArray()).hasSize(expectedSize);
    }
    var stream = new ByteArrayInputStream(bytes.getArray());
    var result = deserializer.deserialize(stream);
    assertThat(result).isEqualTo(original);
    assertThat(stream.available()).isEqualTo(0);

    stream.reset();
    deserializer.skipDeserialize(stream);
    assertThat(stream.available()).isEqualTo(0);
  }
}
