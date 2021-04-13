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
package io.prestosql.plugin.smartnews.functions;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import com.alibaba.fastjson.parser.Feature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.airlift.slice.Murmur3Hash32;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import net.thisptr.jackson.jq.JsonQuery;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public final class ExtendedFunctions
{
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final LoadingCache<String, JsonQuery> JSON_QUERY_CACHE;
    private static LoadingCache<Slice, JSONPath> jsonPathCache = CacheBuilder.newBuilder()
            .maximumSize(500)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build(
                    new CacheLoader<Slice, JSONPath>()
                    {
                        public JSONPath load(Slice jsonPath)
                                throws Exception
                        {
                            return JSONPath.compile(jsonPath.toStringUtf8());
                        }
                    });
    private static HashFunction murmur3 = Hashing.murmur3_32(0x9747b28c);

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
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
    }

    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long hashInto(@SqlType(StandardTypes.VARCHAR) Slice str, @SqlType(StandardTypes.BIGINT) long hashDimension)
    {
        long r = 0;
        r = r * 31 + str.toStringUtf8().hashCode();
        r = r % hashDimension;

        if (r < 0) {
            r += hashDimension;
        }
        return 1 + r;
    }

    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long hashIntoNaive(@SqlType(StandardTypes.VARCHAR) Slice str, @SqlType(StandardTypes.BIGINT) long hashDimension)
    {
        return (Math.abs(str.toStringUtf8().hashCode()) % hashDimension) + 1;
    }

    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long rawMhash(@SqlType(StandardTypes.BIGINT) long seed,
            @SqlType(StandardTypes.VARCHAR) Slice data)
    {
        return Murmur3Hash32.hash((int) seed, data, 0, data.length());
    }

    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long mhash(@SqlType(StandardTypes.VARCHAR) Slice str, @SqlType(StandardTypes.BIGINT) long hashDimension)
    {
        int l = (int) (murmur3.hashString(str.toStringUtf8(), StandardCharsets.UTF_8).asInt() % hashDimension);
        if (l < 0) {
            l += hashDimension;
        }
        return l + 1;
    }

    static {
        JSON_QUERY_CACHE = CacheBuilder.newBuilder()
                .concurrencyLevel(10)
                .maximumSize(100)
                .expireAfterAccess(60, TimeUnit.SECONDS)
                .build(new CacheLoader<String, JsonQuery>()
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
