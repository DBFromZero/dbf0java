package dbf0.disk_key_value.readwrite.log;

import dbf0.common.Dbf0Util;
import dbf0.disk_key_value.io.FileOperations;
import org.junit.Before;
import org.junit.Test;

import java.io.OutputStream;
import java.util.logging.Level;

import static org.mockito.Mockito.*;

public class ImmediateLogSynchronizerTest {

  @Before public void setUp() throws Exception {
    Dbf0Util.enableConsoleLogging(Level.FINER, true);
  }

  @Test public void testScheduledSynchronization() throws Exception {
    var fileOperations = (FileOperations<OutputStream>) mock(FileOperations.class);
    var outputStream = mock(OutputStream.class);

    var factory = ImmediateLogSynchronizer.factory();
    var synchronizer = factory.create(fileOperations, outputStream);

    verifyZeroInteractions(fileOperations);

    synchronizer.registerLog();
    verify(fileOperations, times(1)).sync(outputStream);

    synchronizer.close();
    verify(fileOperations, times(1)).sync(outputStream);
  }
}
