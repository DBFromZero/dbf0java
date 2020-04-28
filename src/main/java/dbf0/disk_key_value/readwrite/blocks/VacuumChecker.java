package dbf0.disk_key_value.readwrite.blocks;

public interface VacuumChecker {

  boolean vacuumNeeded();

  static VacuumChecker never() {
    return () -> false;
  }
}
