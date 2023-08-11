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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import com.alibaba.fastjson.parser.Feature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.airlift.slice.Murmur3Hash32;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.collect.cache.EvictableCacheBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import net.thisptr.jackson.jq.JsonQuery;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public final class ExtendedFunctions
{
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final LoadingCache<String, JsonQuery> JSON_QUERY_CACHE;
    private static LoadingCache<Slice, JSONPath> jsonPathCache = EvictableCacheBuilder.newBuilder()
            .expireAfterWrite(10 * 60 * 1000L, TimeUnit.MILLISECONDS)
            .maximumSize(500L)
            .build(
                new CacheLoader<Slice, JSONPath>()
                {
                    public JSONPath load(Slice jsonPath)
                            throws Exception
                    {
                        return JSONPath.compile(jsonPath.toStringUtf8());
                    }
                });
    private static HashFunction murmur3 = Hashing.murmur3_32_fixed(0x9747b28c);

    private ExtendedFunctions()
    {
        //no instance
    }

    @ScalarFunction("fj")
    @Description("com.alibaba.fastjson -based JSON extraction: j(JSON_OBJECT, 'JSON_PATH') (= fast version of json_extract_scalar)")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice varcharFastJsonExtract(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(StandardTypes.VARCHAR) Slice jsonPath)
    {
        try {
            return jsonGet(json.toStringUtf8(), jsonPathCache.get(jsonPath));
        }
        catch (ExecutionException e) {
            return null;
        }
    }

    @ScalarFunction("fj")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice jsonFastJsonExtract(@SqlType(StandardTypes.JSON) Slice json, @SqlType(StandardTypes.VARCHAR) Slice jsonPath)
    {
        try {
            return jsonGet(json.toStringUtf8(), jsonPathCache.get(jsonPath));
        }
        catch (ExecutionException e) {
            return null;
        }
    }

    private static Slice jsonGet(String json, JSONPath jsonPath)
    {
        if (json != null) {
            JSONObject o = JSON.parseObject(json,
                    Feature.DisableCircularReferenceDetect);

            if (o != null) {
                Object extracted = jsonPath.eval(o);
                if (extracted != null) {
                    return Slices.utf8Slice(String.valueOf(extracted));
                }
            }
        }

        return null;
    }

    @ScalarFunction("jq")
    @SqlNullable
    @SqlType(StandardTypes.JSON)
    public static Slice jq(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(StandardTypes.VARCHAR) Slice jsonQuery)
    {
        try {
            JsonQuery query = JSON_QUERY_CACHE.get(jsonQuery.toStringUtf8());
            JsonNode node = objectMapper.readTree(json.toStringUtf8());
            List<JsonNode> result = query.apply(node);
            return Slices.utf8Slice(objectMapper.writeValueAsString(result));
        }
        catch (IOException | ExecutionException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
    }

    @ScalarFunction("hash_into")
    @SqlType(StandardTypes.BIGINT)
    public static long hashInto(@SqlType(StandardTypes.VARCHAR) Slice str, @SqlType(StandardTypes.BIGINT) long hashDimension)
    {
        long r = str.toStringUtf8().hashCode() % hashDimension;

        if (r < 0) {
            r += hashDimension;
        }
        return 1 + r;
    }

    @ScalarFunction("raw_mhash")
    @SqlType(StandardTypes.BIGINT)
    public static long rawMhash(@SqlType(StandardTypes.BIGINT) long seed,
            @SqlType(StandardTypes.VARCHAR) Slice data)
    {
        return Murmur3Hash32.hash((int) seed, data, 0, data.length());
    }

    public static long mhashImpl(String str, long hashDimension)
    {
        int l = (int) (murmur3.hashString(str, StandardCharsets.UTF_8).asInt() % hashDimension);
        if (l < 0) {
            l += hashDimension;
        }
        return l + 1;
    }

    @Description("_FUNC_(string word) returns a murmurhash3 INT value starting from 1")
    @ScalarFunction("mhash")
    @SqlType(StandardTypes.INTEGER)
    public static long mhash(@SqlType(StandardTypes.VARCHAR) Slice str, @SqlType(StandardTypes.BIGINT) long hashDimension)
    {
        return mhashImpl(str.toStringUtf8(), hashDimension);
    }

    @Description("_FUNC_(string word) returns a murmurhash3 INT value starting from 1")
    @ScalarFunction("mhash")
    @SqlType(StandardTypes.INTEGER)
    public static long mur3Hash(@SqlType(StandardTypes.VARCHAR) Slice argument) throws SQLException
    {
        String str = argument.toStringUtf8();
        return mhashImpl(str, 16777216);
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
            return mhashImpl(sb.toString(), seed);
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

    static {
        JSON_QUERY_CACHE = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(60 * 1000L, TimeUnit.MILLISECONDS)
                .maximumSize(100L)
                .build(
                    new CacheLoader<String, JsonQuery>()
                    {
                        @Override
                        public JsonQuery load(String jsonQuery)
                                throws Exception
                        {
                            return JsonQuery.compile(jsonQuery);
                        }
                    });
    }
}
