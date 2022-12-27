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

import com.google.common.hash.Hashing;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.sql.SQLException;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class SmartnewsHash
{
    private static final String HASH_KEY = Optional.ofNullable(System.getenv("SN_HASH_KEY")).orElse("smartnews");

    private SmartnewsHash()
    {
    }

    @Description("Dynamic consistent hash")
    @ScalarFunction("sn_hash")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice hashStr(ConnectorSession session, @SqlType(StandardTypes.VARCHAR) Slice slice) throws SQLException
    {
        String argument = slice.toStringUtf8();
        return Slices.utf8Slice(sha256(HASH_KEY + argument.toString() + session.getQueryId()));
    }

    @Description("Dynamic consistent hash")
    @ScalarFunction("sn_hash")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice hashInt(ConnectorSession session, @SqlType(StandardTypes.INTEGER) long argument) throws SQLException
    {
        return Slices.utf8Slice(sha256(HASH_KEY + Long.toHexString(argument) + session.getQueryId()));
    }

    @Description("Dynamic consistent hash")
    @ScalarFunction("sn_hash")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice hashInt(ConnectorSession session, @SqlType(StandardTypes.DOUBLE) double argument) throws SQLException
    {
        return Slices.utf8Slice(sha256(HASH_KEY + Double.toHexString(argument) + session.getQueryId()));
    }

    private static String sha256(String input)
    {
        return Hashing.sha256().hashString(input, UTF_8).toString();
    }
}
