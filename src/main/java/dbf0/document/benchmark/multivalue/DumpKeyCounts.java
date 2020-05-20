package dbf0.document.benchmark.multivalue;

import com.google.common.base.Joiner;
import dbf0.disk_key_value.readonly.multivalue.KeyMultiValueFileReader;
import dbf0.disk_key_value.readwrite.lsmtree.multivalue.ValueWrapper;
import dbf0.document.serialization.DElementDeserializer;
import dbf0.document.types.DElement;
import dbf0.document.types.DString;
import org.apache.commons.lang3.tuple.Pair;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Map;

public class DumpKeyCounts {

  public static void main(String[] args) throws Exception {
    var stream = new FileInputStream("/data/tmp/document_testing/0/base");
    var reader = KeyMultiValueFileReader.bufferStream(DElementDeserializer.defaultCharsetInstance(),
        ValueWrapper.serializationPair(DElement.sizePrefixedSerializationPair()).getDeserializer(),
        stream);
    var acc = new ArrayList<Pair<String, Integer>>();
    while (true) {
      var key = reader.readKey();
      if (key == null) {
        break;
      }
      var count = reader.getValuesCount();
      acc.add(Pair.of(((DString) key).getValue(), count));
      System.out.println(Joiner.on(" = ").join(key, count));
      reader.skipRemainingValues();
    }
    System.out.println();
    System.out.println(acc.size());
    acc.sort(Map.Entry.comparingByValue());
    acc.stream().skip(acc.size() - 20).forEach(System.out::println);
  }
}
