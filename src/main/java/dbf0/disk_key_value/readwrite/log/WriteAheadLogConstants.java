package dbf0.disk_key_value.readwrite.log;

public class WriteAheadLogConstants {

  public static final byte PUT = (byte) 0x10;
  public static final byte DELETE = (byte) 0x20;

  private WriteAheadLogConstants() {
  }
}
