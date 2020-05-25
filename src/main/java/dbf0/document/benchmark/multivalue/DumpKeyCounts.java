package dbf0.document.benchmark.multivalue;

import dbf0.common.Dbf0Util;
import dbf0.disk_key_value.readonly.multivalue.KeyMultiValueFileReader;
import dbf0.disk_key_value.readwrite.lsmtree.multivalue.ValueWrapper;
import dbf0.document.serialization.DElementDeserializer;
import dbf0.document.types.DElement;
import dbf0.document.types.DString;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DumpKeyCounts {

  private static final Logger LOGGER = Dbf0Util.getLogger(ParseAndPruneDocuments.class);

  public static void main(String[] args) throws Exception {
    Dbf0Util.enableConsoleLogging(Level.FINE, true);
    var counts = new HashMap<String, Long>(2048);
    for (String path : args) {
      LOGGER.info("Reading " + path);
      try (var stream = new FileInputStream(path)) {
        var reader = KeyMultiValueFileReader.bufferStream(DElementDeserializer.defaultCharsetInstance(),
            ValueWrapper.serializationPair(DElement.serializationPair()).getDeserializer(),
            stream);
        while (true) {
          var dKey = reader.readKey();
          if (dKey == null) {
            break;
          }
          var count = reader.getValuesCount();
          String key = ((DString) dKey).getValue();
          counts.put(key, Optional.ofNullable(counts.get(key)).orElse(0L) + count);
          reader.skipRemainingValues();
        }
      }
    }
    var acc = new ArrayList<>(counts.entrySet());
    acc.sort(Map.Entry.comparingByValue());
    acc.forEach(System.out::println);
  }
}
