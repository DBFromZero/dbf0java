package dbf0.disk_key_value.readwrite.blocks;

import dbf0.common.Dbf0Util;

import java.util.logging.Logger;

public class VacuumCheckerFractionUnused implements VacuumChecker {

  private static final Logger LOGGER = Dbf0Util.getLogger(VacuumCheckerFractionUnused.class);

  private final BlockStorage storage;
  private final double unusedFractionThreshold;
  private final int frequency;
  private int checkCounter;

  public VacuumCheckerFractionUnused(BlockStorage storage, double unusedFractionThreshold, int frequency) {
    this.storage = storage;
    this.unusedFractionThreshold = unusedFractionThreshold;
    this.frequency = frequency;
  }

  @Override public boolean vacuumNeeded() {
    if (checkCounter++ != frequency) {
      return false;
    }
    checkCounter = 0;
    var stats = storage.getStats();
    var unusedFraction = (double) stats.getUnused().getBytes() / (double) (stats.getUnused().getBytes() + stats.getUsed().getBytes());
    LOGGER.fine(() -> String.format("Unused storage fraction %.1f%%", unusedFraction * 100));
    return unusedFraction > unusedFractionThreshold;
  }
}
