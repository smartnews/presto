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
package com.facebook.presto.smartnews.functions;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import com.alibaba.fastjson.parser.Feature;
import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by yuyanglan on 2/1/16.
 */
public final class ExtendedFunctions
{
    private static LoadingCache<Slice, JSONPath> jsonPathCache = CacheBuilder.newBuilder()
            .maximumSize(500)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build(
                    new CacheLoader<Slice, JSONPath>()
                    {
                        public JSONPath load(Slice jsonPath) throws Exception
                        {
                            return JSONPath.compile(jsonPath.toStringUtf8());
                        }
                    });

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

    public static Slice jsonGet(String json, JSONPath jsonPath)
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
}
