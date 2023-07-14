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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.util.HashMap;
import java.util.Map;

public class JsonFunctions
{
    static ObjectMapper objectMapper = new ObjectMapper();

    private static final TypeReference<Map<String, Object>> TYPE_REFERENCE = new TypeReference<Map<String, Object>>() {};

    private JsonFunctions()
    {
    }

    @Description("Merge multiple json string into one")
    @ScalarFunction(value = "merge_json")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice mergeJson2(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice1,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice2)
            throws JsonProcessingException
    {
        Map<String, Object> result = new HashMap<>();
        merge(result, slice1);
        merge(result, slice2);
        return Slices.utf8Slice(objectMapper.writeValueAsString(result));
    }

    @Description("Merge multiple json string into one")
    @ScalarFunction("merge_json")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice mergeJson3(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice1,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice2,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice3)
            throws JsonProcessingException
    {
        Map<String, Object> result = new HashMap<>();
        merge(result, slice1);
        merge(result, slice2);
        merge(result, slice3);
        return Slices.utf8Slice(objectMapper.writeValueAsString(result));
    }

    @Description("Merge multiple json string into one")
    @ScalarFunction("merge_json")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice mergeJson4(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice1,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice2,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice3,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice4)
            throws JsonProcessingException
    {
        Map<String, Object> result = new HashMap<>();
        merge(result, slice1);
        merge(result, slice2);
        merge(result, slice3);
        merge(result, slice4);
        return Slices.utf8Slice(objectMapper.writeValueAsString(result));
    }

    @Description("Merge multiple json string into one")
    @ScalarFunction("merge_json")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice mergeJson5(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice1,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice2,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice3,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice4,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice5)
            throws JsonProcessingException
    {
        Map<String, Object> result = new HashMap<>();
        merge(result, slice1);
        merge(result, slice2);
        merge(result, slice3);
        merge(result, slice4);
        merge(result, slice5);
        return Slices.utf8Slice(objectMapper.writeValueAsString(result));
    }

    @Description("Merge multiple json string into one")
    @ScalarFunction("merge_json")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice mergeJson6(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice1,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice2,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice3,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice4,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice5,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice6)
            throws JsonProcessingException
    {
        Map<String, Object> result = new HashMap<>();
        merge(result, slice1);
        merge(result, slice2);
        merge(result, slice3);
        merge(result, slice4);
        merge(result, slice5);
        merge(result, slice6);
        return Slices.utf8Slice(objectMapper.writeValueAsString(result));
    }

    @Description("Merge multiple json string into one")
    @ScalarFunction("merge_json")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice mergeJson7(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice1,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice2,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice3,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice4,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice5,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice6,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice7)
            throws JsonProcessingException
    {
        Map<String, Object> result = new HashMap<>();
        merge(result, slice1);
        merge(result, slice2);
        merge(result, slice3);
        merge(result, slice4);
        merge(result, slice5);
        merge(result, slice6);
        merge(result, slice7);
        return Slices.utf8Slice(objectMapper.writeValueAsString(result));
    }

    @Description("Merge multiple json string into one")
    @ScalarFunction("merge_json")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice mergeJson8(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice1,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice2,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice3,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice4,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice5,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice6,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice7,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice8)
            throws JsonProcessingException
    {
        Map<String, Object> result = new HashMap<>();
        merge(result, slice1);
        merge(result, slice2);
        merge(result, slice3);
        merge(result, slice4);
        merge(result, slice5);
        merge(result, slice6);
        merge(result, slice7);
        merge(result, slice8);
        return Slices.utf8Slice(objectMapper.writeValueAsString(result));
    }

    @Description("Merge multiple json string into one")
    @ScalarFunction("merge_json")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice mergeJson9(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice1,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice2,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice3,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice4,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice5,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice6,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice7,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice8,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice9)
            throws JsonProcessingException
    {
        Map<String, Object> result = new HashMap<>();
        merge(result, slice1);
        merge(result, slice2);
        merge(result, slice3);
        merge(result, slice4);
        merge(result, slice5);
        merge(result, slice6);
        merge(result, slice7);
        merge(result, slice8);
        merge(result, slice9);
        return Slices.utf8Slice(objectMapper.writeValueAsString(result));
    }

    @Description("Merge multiple json string into one")
    @ScalarFunction("merge_json")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice mergeJson10(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice1,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice2,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice3,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice4,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice5,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice6,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice7,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice8,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice9,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice10)
            throws JsonProcessingException
    {
        Map<String, Object> result = new HashMap<>();
        merge(result, slice1);
        merge(result, slice2);
        merge(result, slice3);
        merge(result, slice4);
        merge(result, slice5);
        merge(result, slice6);
        merge(result, slice7);
        merge(result, slice8);
        merge(result, slice9);
        merge(result, slice10);
        return Slices.utf8Slice(objectMapper.writeValueAsString(result));
    }

    @Description("Merge multiple json string into one")
    @ScalarFunction("merge_json")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice mergeJson11(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice1,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice2,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice3,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice4,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice5,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice6,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice7,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice8,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice9,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice10,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice11)
            throws JsonProcessingException
    {
        Map<String, Object> result = new HashMap<>();
        merge(result, slice1);
        merge(result, slice2);
        merge(result, slice3);
        merge(result, slice4);
        merge(result, slice5);
        merge(result, slice6);
        merge(result, slice7);
        merge(result, slice8);
        merge(result, slice9);
        merge(result, slice10);
        merge(result, slice11);
        return Slices.utf8Slice(objectMapper.writeValueAsString(result));
    }

    @Description("Merge multiple json string into one")
    @ScalarFunction("merge_json")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice mergeJson12(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice1,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice2,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice3,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice4,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice5,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice6,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice7,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice8,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice9,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice10,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice11,
                                    @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice slice12)
            throws JsonProcessingException
    {
        Map<String, Object> result = new HashMap<>();
        merge(result, slice1);
        merge(result, slice2);
        merge(result, slice3);
        merge(result, slice4);
        merge(result, slice5);
        merge(result, slice6);
        merge(result, slice7);
        merge(result, slice8);
        merge(result, slice9);
        merge(result, slice10);
        merge(result, slice11);
        merge(result, slice12);
        return Slices.utf8Slice(objectMapper.writeValueAsString(result));
    }

    public static void merge(Map<String, Object> result, Slice slice)
    {
        if (slice == null) {
            return;
        }
        String argument = slice.toStringUtf8();
        if (argument == null || argument.isEmpty()) {
            return;
        }
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }

        Map<String, Object> map = null;
        try {
            map = objectMapper.readValue(argument, TYPE_REFERENCE);
        }
        catch (Exception e) {
            //e.printStackTrace();
        }

        if (map == null) {
            return;
        }

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            // assert !result.containsKey(entry.getKey()) || entry.getValue().equals(result.get(entry.getKey()));

            if (entry.getValue() != null) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
    }
}
