/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.smartnews.functions;

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

/**
 * Migrate from <a href="https://github.com/smartnews/smart-ad-dmp-udf/blob/master/src/main/java/com/smartnews/ad/common/XxHash32.java">Hive UDF</a>
 */
public final class XxHash32Functions
{
    private XxHash32Functions() {}

    @Description("Compute 32-bit xxHash of a UTF-8 string with the given seed")
    @ScalarFunction("xxhash32")
    @SqlType(StandardTypes.INTEGER)
    public static long xxhash32(
            @SqlType(StandardTypes.VARCHAR) Slice slice,
            @SqlType(StandardTypes.INTEGER) long seed)
    {
        byte[] bytes = slice.getBytes();
        int intSeed = (int) seed;
        return hash(bytes, intSeed);
    }

    @Description("Compute 32-bit xxHash of a UTF-8 string with seed 0")
    @ScalarFunction("xxhash32")
    @SqlType(StandardTypes.INTEGER)
    public static long xxhash32(
            @SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        return xxhash32(slice, 0L);
    }

    private static int hash(byte[] bytes, int seed)
    {
        int off = 0;
        int len = bytes.length;
        int h32;

        if (len >= 16) {
            int limit = len - 16;
            int v1 = seed + -1640531535 + -2048144777;
            int v2 = seed + -2048144777;
            int v3 = seed;
            int v4 = seed - -1640531535;

            do {
                v1 += readIntLE(bytes, off) * -2048144777;
                v1 = Integer.rotateLeft(v1, 13);
                v1 *= -1640531535;
                off += 4;

                v2 += readIntLE(bytes, off) * -2048144777;
                v2 = Integer.rotateLeft(v2, 13);
                v2 *= -1640531535;
                off += 4;

                v3 += readIntLE(bytes, off) * -2048144777;
                v3 = Integer.rotateLeft(v3, 13);
                v3 *= -1640531535;
                off += 4;

                v4 += readIntLE(bytes, off) * -2048144777;
                v4 = Integer.rotateLeft(v4, 13);
                v4 *= -1640531535;
                off += 4;
            }
            while (off <= limit);

            h32 = Integer.rotateLeft(v1, 1)
                    + Integer.rotateLeft(v2, 7)
                    + Integer.rotateLeft(v3, 12)
                    + Integer.rotateLeft(v4, 18);
        }
        else {
            h32 = seed + 374761393;
        }

        h32 += len;

        while (off <= len - 4) {
            h32 += readIntLE(bytes, off) * -1028477379;
            h32 = Integer.rotateLeft(h32, 17) * 668265263;
            off += 4;
        }

        while (off < len) {
            h32 += (readByte(bytes, off) & 0xFF) * 374761393;
            h32 = Integer.rotateLeft(h32, 11) * -1640531535;
            off++;
        }

        h32 ^= (h32 >>> 15);
        h32 *= -2048144777;
        h32 ^= (h32 >>> 13);
        h32 *= -1028477379;
        h32 ^= (h32 >>> 16);

        return h32;
    }

    private static byte readByte(byte[] buf, int i)
    {
        return buf[i];
    }

    private static int readIntLE(byte[] buf, int i)
    {
        return (buf[i] & 0xFF)
                | ((buf[i + 1] & 0xFF) << 8)
                | ((buf[i + 2] & 0xFF) << 16)
                | ((buf[i + 3] & 0xFF) << 24);
    }
}
