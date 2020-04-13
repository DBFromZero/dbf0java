package dbf0.mem_key_value;

import dbf0.ByteArrayWrapper;

interface KeyValueSource {

  ByteArrayWrapper generateKey();

  ByteArrayWrapper generateValueForKey(ByteArrayWrapper key);
}