/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cores.core;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * Implements CRC32 checksum.
 */
final class Crc32Checksum extends Checksum {
    private CRC32 crc32 = new CRC32();

    @Override public int size() {
        return 4;
    }

    @Override public ByteBuffer compute(ByteBuffer data) {
        crc32.reset();
        crc32.update(data.array(), data.position(), data.remaining());

        ByteBuffer result = ByteBuffer.allocate(size());
        result.putInt((int) crc32.getValue());
        result.flip();
        return result;
    }

}
