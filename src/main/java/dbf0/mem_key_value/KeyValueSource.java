package dbf0.mem_key_value;

import dbf0.common.ByteArrayWrapper;

interface KeyValueSource {

  ByteArrayWrapper generateKey();

  ByteArrayWrapper generateValueForKey(ByteArrayWrapper key);
}