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
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.smartnews.dmp.core.util.hash.SimHash;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
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

    private static final String SimHashLock = "";

    private static SimHash<String> simHash;

    private HashFunctions()
    {
        simHash = SimHash.build();
    }

    @Description("Dynamic consistent hash")
    @ScalarFunction("sn_hash")
    @LiteralParameters("x")
    @SqlType("char(x)")
    public static Slice hashChars(@LiteralParameter("x") Long x, ConnectorSession session, @SqlType("char(x)") Slice slice) throws SQLException
    {
        int xInt = x.intValue();
        String hashed = generateSnHashCode(session, slice.toStringUtf8()).toString();
        return Slices.utf8Slice((SN_HASH_PREFIX + hashed).substring(0, SN_HASH_LENGTH < xInt ? SN_HASH_LENGTH : xInt));
    }

    @Description("Dynamic consistent hash")
    @ScalarFunction("sn_hash")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice hashVarchar(ConnectorSession session, @SqlType("varchar") Slice slice) throws SQLException
    {
        String hashed = SN_HASH_PREFIX + generateSnHashCode(session, slice.toStringUtf8()).toString();
        return Slices.utf8Slice(hashed.substring(0, SN_HASH_LENGTH));
    }

    @Description("Dynamic consistent hash")
    @ScalarFunction("sn_hash")
    @SqlType(StandardTypes.INTEGER)
    public static long hashInt(ConnectorSession session, @SqlType(StandardTypes.INTEGER) long argument) throws SQLException
    {
        long res = generateSnHashCode(session, argument).asInt();
        return res > 0 ? -res : res;
    }

    @Description("Dynamic consistent hash")
    @ScalarFunction("sn_hash")
    @SqlType(StandardTypes.INTEGER)
    public static long hashBigint(ConnectorSession session, @SqlType(StandardTypes.BIGINT) long argument) throws SQLException
    {
        long res = generateSnHashCode(session, argument).asInt();
        return res > 0 ? -res : res;
    }

    @Description("Dynamic consistent hash")
    @ScalarFunction("sn_hash")
    @SqlType(StandardTypes.DOUBLE)
    public static double hashDouble(ConnectorSession session, @SqlType(StandardTypes.DOUBLE) double argument) throws SQLException
    {
        long res = generateSnHashCode(session, Double.doubleToRawLongBits(argument)).asInt();
        return res > 0 ? -res : res;
    }

    private static HashCode generateSnHashCode(ConnectorSession session, long content)
    {
        if (SN_HASH_KEY == null || SN_HASH_KEY.length() == 0) {
            throw new RuntimeException("Env SN_HASH_KEY not found.");
        }
        return Hashing.murmur3_128((int) session.getStart().toEpochMilli())
                .newHasher()
                .putUnencodedChars(SN_HASH_KEY)
                .putLong(content)
                .hash();
    }

    private static HashCode generateSnHashCode(ConnectorSession session, String content)
    {
        if (SN_HASH_KEY == null || SN_HASH_KEY.length() == 0) {
            throw new RuntimeException("Env SN_HASH_KEY not found.");
        }
        return Hashing.murmur3_128((int) session.getStart().toEpochMilli())
                .newHasher()
                .putUnencodedChars(SN_HASH_KEY)
                .putUnencodedChars(content)
                .hash();
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

        byte[] ret = null;
        synchronized (SimHashLock) {
            // within simHash, there is a cache of type HashMap.
            // HashMap<T, BitSet> hashingDim, which is not thread safe
            // so we need to synchronize here.
            if (simHash == null) {
                simHash = SimHash.build();
            }
            ret = simHash.simHash(m).toByteArray();
        }
        return Slices.wrappedBuffer(ret);
    }

    @VisibleForTesting
    static void setSnHashKey(String key)
    {
        SN_HASH_KEY = key;
    }
}
