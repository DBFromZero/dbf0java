package dbf0.document.benchmark.multivalue;

import dbf0.common.Dbf0Util;
import dbf0.disk_key_value.readonly.multivalue.KeyMultiValueFileReader;
import dbf0.disk_key_value.readwrite.lsmtree.multivalue.ValueWrapper;
import dbf0.document.serialization.DElementDeserializer;
import dbf0.document.types.DElement;
import dbf0.document.types.DString;

import java.io.FileInputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DumpFiles {

  private static final Logger LOGGER = Dbf0Util.getLogger(ParseAndPruneDocuments.class);

  public static void main(String[] args) throws Exception {
    Dbf0Util.enableConsoleLogging(Level.FINE, true);
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
          String key = ((DString) dKey).getValue();
          reader.valueIterator().forEachRemaining(value -> System.out.println(key + "=" + value.getValue()));
        }
      }
    }
  }
}
