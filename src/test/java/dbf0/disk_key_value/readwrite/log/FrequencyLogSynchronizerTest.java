package dbf0.disk_key_value.readwrite.log;

import dbf0.common.Dbf0Util;
import dbf0.disk_key_value.io.FileOperations;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.OutputStream;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class FrequencyLogSynchronizerTest {

  @Before public void setUp() throws Exception {
    Dbf0Util.enableConsoleLogging(Level.FINER, true);
  }

  @Test public void testScheduledSynchronization() throws Exception {
    var frequency = Duration.ofMillis(32094);
    var executor = mock(ScheduledExecutorService.class);
    var future = mock(ScheduledFuture.class);
    var fileOperations = (FileOperations<OutputStream>) mock(FileOperations.class);
    var outputStream = mock(OutputStream.class);

    when(executor.scheduleWithFixedDelay(any(Runnable.class), eq(frequency.toMillis()), eq(frequency.toMillis()), eq(TimeUnit.MILLISECONDS)))
        .thenReturn(future);
    var factory = FrequencyLogSynchronizer.factory(executor, frequency);
    var synchronizer = factory.create(fileOperations, outputStream);

    var runnableCapture = ArgumentCaptor.forClass(Runnable.class);
    verify(executor, times(1))
        .scheduleWithFixedDelay(runnableCapture.capture(), eq(frequency.toMillis()), eq(frequency.toMillis()), eq(TimeUnit.MILLISECONDS));
    var runnable = runnableCapture.getValue();
    assertThat(runnable).isNotNull();

    verifyZeroInteractions(fileOperations);

    synchronizer.registerLog();
    runnable.run();
    verify(fileOperations, times(1)).sync(outputStream);

    runnable.run();
    verify(fileOperations, times(1)).sync(outputStream);

    synchronizer.close();
    verify(future, times(1)).cancel(anyBoolean());
  }
}
