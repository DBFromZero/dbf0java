package dbf0.document;

import com.codepoetics.protonpack.StreamUtils;
import dbf0.document.types.*;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class DElementTest {

  private static final List<DElement> EXAMPLE_ELEMENTS = List.of(
      DNull.getInstance(),
      DBool.getTrue(),
      DInt.of(7),
      DDecimal.of(new BigDecimal("3.14")),
      DString.of("string"),
      DArray.of(),
      DMap.of()
  );

  @Test public void testCompareElements() {
    StreamUtils.zipWithIndex(EXAMPLE_ELEMENTS.stream()).forEach(indexA -> {
      var a = indexA.getValue();
      EXAMPLE_ELEMENTS.stream().skip(indexA.getIndex()).forEach(b ->
          assertThat(a.compareTo(b)).isEqualTo(a == b ? 0 :
              Integer.compare(a.getType().getTypeCode(), b.getType().getTypeCode())));
    });
  }
}
