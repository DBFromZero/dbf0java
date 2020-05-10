package dbf0.document;

import dbf0.common.ReservoirSampler;
import dbf0.disk_key_value.io.FileDirectoryOperationsImpl;
import dbf0.disk_key_value.readwrite.ReadWriteStorageWithBackgroundTasks;
import dbf0.disk_key_value.readwrite.lsmtree.LsmTree;
import dbf0.document.gson.DElementTypeAdapter;
import dbf0.document.types.DElement;
import dbf0.document.types.DMap;
import dbf0.document.types.DString;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.util.Random;

public class ExperimentDocumentStore {

  public static void main(String[] args) throws Exception {
    var directoryOperations = new FileDirectoryOperationsImpl(new File("/data/tmp/document_testing"));
    directoryOperations.mkdirs();
    directoryOperations.clear();

    var sampleIds = new ReservoirSampler<DString>(new Random(), 1000);

    try (var store = createStore(directoryOperations)) {
      store.initialize();
      var adapter = DElementTypeAdapter.getInstance();
      for (int i = 0; i < 10; i++) {
        try (var reader = new BufferedReader(new FileReader("/data/tmp/reddit/RS_2019-08.json"))) {
          String line;
          while ((line = reader.readLine()) != null) {
            var element = (DMap) adapter.fromJson(line);
            var id = (DString) element.getEntries().get(DString.of("id"));
            id = DString.of(id.getValue() + "-" + i);
            store.put(id, element);
            sampleIds.add(id);
          }
        }
      }
    }

    try (var store = createStore(directoryOperations)) {
      store.initialize();
      for (var id : sampleIds.getSampled()) {
        var value = store.get(id);
        if (value == null) {
          System.out.println("Missing id " + id);
        }
      }
    }
  }

  private static ReadWriteStorageWithBackgroundTasks<DElement, DElement> createStore(FileDirectoryOperationsImpl directoryOperations) {
    return LsmTree.<FileOutputStream>builderForDocuments()
        .withBaseDeltaFiles(directoryOperations)
        .withPendingWritesDeltaThreshold(500)
        .buildWithBackgroundTasks();
  }
}
