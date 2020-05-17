package dbf0.document.benchmark;

import com.google.common.base.Joiner;
import dbf0.common.io.SizePrefixedDeserializer;
import dbf0.disk_key_value.readonly.KeyValueFileReader;
import dbf0.document.serialization.DElementDeserializer;
import dbf0.document.types.DNull;

import java.io.FileInputStream;

public class DumpFile {

  public static void main(String[] args) throws Exception {
    var stream = new FileInputStream("/data/tmp/document_testing/0/delta-96");
    var reader = KeyValueFileReader.bufferStream(DElementDeserializer.defaultCharsetInstance(),
        new SizePrefixedDeserializer<>(DElementDeserializer.defaultCharsetInstance(), false), stream);
    while (true) {
      var key = reader.readKey();
      if (key == null) {
        break;
      }
      var value = DNull.getInstance();
      reader.skipValue();
      System.out.println(Joiner.on(" = ").join(key, value));
    }
  }
}
