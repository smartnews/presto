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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hashing;
import com.smartnews.dmp.core.util.hash.SimHash;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.StandardTypes;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;

public final class HashFunctions
{
    @SuppressWarnings("checkstyle:StaticVariableName")
    private static String SN_HASH_KEY = System.getenv("SN_HASH_KEY");
    public static final String SN_HASH_PREFIX = "H_";
    public static final int SN_HASH_LENGTH = 20;

    private static SimHash<String> simHash;

    private HashFunctions()
    {
        simHash = SimHash.build();
    }

    @Description("Dynamic consistent hash")
    @ScalarFunction("sn_hash")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice hashStr(ConnectorSession session, @SqlType(StandardTypes.VARCHAR) Slice slice) throws SQLException
    {
        return genSnHashSlice(slice.toStringUtf8() + session.getQueryId());
    }

    @Description("Dynamic consistent hash")
    @ScalarFunction("sn_hash")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice hashInt(ConnectorSession session, @SqlType(StandardTypes.INTEGER) long argument) throws SQLException
    {
        return genSnHashSlice(Long.toHexString(argument) + session.getQueryId());
    }

    @Description("Dynamic consistent hash")
    @ScalarFunction("sn_hash")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice hashInt(ConnectorSession session, @SqlType(StandardTypes.DOUBLE) double argument) throws SQLException
    {
        return genSnHashSlice(Double.toHexString(argument) + session.getQueryId());
    }

    @Description("_FUNC_(string word) returns a murmurhash3 INT value starting from 1")
    @ScalarFunction("mhash")
    @SqlType(StandardTypes.INTEGER)
    public static long mur3Hash(@SqlType(StandardTypes.VARCHAR) Slice argument) throws SQLException
    {
        String str = argument.toStringUtf8();
        return mhash(str, 16777216);
    }

    @Description("_FUNC_(string word) returns a murmurhash3 INT value starting from 1")
    @ScalarFunction("mhash")
    @SqlType(StandardTypes.INTEGER)
    public static long mur3Hash(@SqlType(StandardTypes.VARCHAR) Slice argument, @SqlType(StandardTypes.INTEGER) long seed) throws SQLException
    {
        String str = argument.toStringUtf8();
        return mhash(str, (int) seed);
    }

    @Description("_FUNC_(string word) returns a murmurhash3 INT value starting from 1")
    @ScalarFunction("mhash")
    @SqlType(StandardTypes.INTEGER)
    public static long mur3Hash(@SqlType("array(varchar)") Block argument, @SqlType(StandardTypes.INTEGER) long seed) throws SQLException
    {
        if (argument instanceof VariableWidthBlock variableWidthBlock) {
            int count = variableWidthBlock.getPositionCount();
            if (count <= 0) {
                return 1;
            }
            StringBuilder sb = new StringBuilder();

            String str = null;
            if (!variableWidthBlock.isNull(0)) {
                int len = variableWidthBlock.getSliceLength(0);
                Slice strSlice = variableWidthBlock.getSlice(0, 0, len);
                str = strSlice == null ? null : strSlice.toStringUtf8();
            }
            sb.append(str);
            for (int i = 1; i < count; i++) {
                str = null;
                if (!variableWidthBlock.isNull(i)) {
                    int len = variableWidthBlock.getSliceLength(i);
                    Slice strSlice = variableWidthBlock.getSlice(i, 0, len);
                    str = strSlice == null ? null : strSlice.toStringUtf8();
                }
                sb.append("\t").append(str);
            }
            return mhash(sb.toString(), (int) seed);
        }
        throw new SQLException("mhash only support array(varchar) type");
    }

    @Description("_FUNC_(string word) returns a murmurhash3 INT value starting from 1")
    @ScalarFunction("mhash")
    @SqlType(StandardTypes.INTEGER)
    public static long mur3Hash(@SqlType("array(varchar)") Block argument) throws SQLException
    {
        return mur3Hash(argument, 16777216);
    }

    @Description("_FUNC_(fv) - Calculate SimHash on a Map<String, Double>")
    @ScalarFunction("simhash")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice simHash(@SqlType("map(varchar,double)") Block mapblock) throws SQLException
    {
        Map<String, Double> m = new HashMap<>();
        if (mapblock != null) {
            for (int keyPosition = 0; keyPosition < mapblock.getPositionCount(); keyPosition += 2) {
                Object keyObj = readNativeValue(VARCHAR, mapblock, keyPosition);
                Object valueObj = readNativeValue(DoubleType.DOUBLE, mapblock, keyPosition + 1);
                if (keyObj instanceof Slice key && valueObj instanceof Double value) {
                    m.put(key.toStringUtf8(), value);
                }
            }
        }

        if (simHash == null) {
            simHash = SimHash.build();
        }

        byte[] ret = simHash.simHash(m).toByteArray();
        return Slices.wrappedBuffer(ret);
    }

    @VisibleForTesting
    static void setSnHashKey(String key)
    {
        SN_HASH_KEY = key;
    }

    @SuppressWarnings("checkstyle:IllegalToken")
    private static Slice genSnHashSlice(String input)
    {
        assert SN_HASH_KEY != null && SN_HASH_KEY.length() > 0 : "Env SN_HASH_KEY not found.";
        String hashed = SN_HASH_PREFIX + Hashing.sha256().hashUnencodedChars(SN_HASH_KEY + input).toString();
        String trimmed = hashed.substring(0, SN_HASH_LENGTH);
        return Slices.utf8Slice(trimmed);
    }

    public static int mhash(String word, int numFeatures)
    {
        int r = murmurhash3X8632(word, 0, word.length(), -1756908916) % numFeatures;
        if (r < 0) {
            r += numFeatures;
        }
        return r + 1;
    }

    public static int murmurhash3X8632(CharSequence data, int offset, int len, int seed)
    {
        int c1 = -862048943;
        int c2 = 461845907;
        int h1 = seed;
        int end = offset + len;
        int pos = offset;
        int k1 = 0;
        int k2 = 0;
        int shift = 0;
        int bits = 0;
        int nBytes = 0;
        while (pos < end) {
            int code = data.charAt(pos++);
            if (code < 128) {
                k2 = code;
                bits = 8;
            }
            else if (code < 2048) {
                k2 = 0xC0 | code >> 6 | (0x80 | code & 0x3F) << 8;
                bits = 16;
            }
            else if (code < 55296 || code > 57343 || pos >= end) {
                k2 = 0xE0 | code >> 12 | (0x80 | code >> 6 & 0x3F) << 8 | (0x80 | code & 0x3F) << 16;
                bits = 24;
            }
            else {
                int utf32 = data.charAt(pos++);
                utf32 = (code - 55232 << 10) + (utf32 & 0x3FF);
                k2 = 0xFF & (0xF0 | utf32 >> 18) | (0x80 | utf32 >> 12 & 0x3F) << 8 | (0x80 | utf32 >> 6 & 0x3F) << 16 | (0x80 | utf32 & 0x3F) << 24;
                bits = 32;
            }
            k1 |= k2 << shift;
            shift += bits;
            if (shift >= 32) {
                k1 *= -862048943;
                k1 = k1 << 15 | k1 >>> 17;
                k1 *= 461845907;
                h1 ^= k1;
                h1 = h1 << 13 | h1 >>> 19;
                h1 = h1 * 5 + -430675100;
                shift -= 32;
                if (shift != 0) {
                    k1 = k2 >>> bits - shift;
                }
                else {
                    k1 = 0;
                }
                nBytes += 4;
            }
        }
        if (shift > 0) {
            nBytes += shift >> 3;
            k1 *= -862048943;
            k1 = k1 << 15 | k1 >>> 17;
            k1 *= 461845907;
            h1 ^= k1;
        }
        h1 ^= nBytes;
        h1 ^= h1 >>> 16;
        h1 *= -2048144789;
        h1 ^= h1 >>> 13;
        h1 *= -1028477387;
        h1 ^= h1 >>> 16;
        return h1;
    }
}
