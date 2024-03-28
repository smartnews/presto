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
package io.trino.plugin.smartnews.functions.actions;

import com.smartnews.dmp.core.data.actions.ActionBase;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.VarcharType.VARCHAR;

public class Utils
{
    private Utils()
    {
    }

    public static class TypeAndName
    {
        public final String name;
        public final Type type;

        public TypeAndName(String name, Type type)
        {
            this.name = name;
            this.type = type;
        }

        public static TypeAndName of(String name, Type type)
        {
            return new TypeAndName(name, type);
        }
    }

    public static List<TypeAndName> baseTypes = Arrays.asList(
            TypeAndName.of("device_token", VarcharType.VARCHAR),
            TypeAndName.of("virtual_id", VarcharType.VARCHAR),
            TypeAndName.of("edition", VarcharType.VARCHAR),
            TypeAndName.of("platform", VarcharType.VARCHAR),
            TypeAndName.of("session_id", VarcharType.VARCHAR),
            TypeAndName.of("app_version", VarcharType.VARCHAR),
            TypeAndName.of("system_version", VarcharType.VARCHAR),
            TypeAndName.of("connection_type", VarcharType.VARCHAR),
            TypeAndName.of("locale", VarcharType.VARCHAR),
            TypeAndName.of("timezone", VarcharType.VARCHAR),
            TypeAndName.of("hardware", VarcharType.VARCHAR),
            TypeAndName.of("event_timestamp", BigintType.BIGINT),
            TypeAndName.of("account_id", VarcharType.VARCHAR),
            TypeAndName.of("user_creation_timestamp", BigintType.BIGINT),
            TypeAndName.of("ab_test", new ArrayType(new MapType(VARCHAR, IntegerType.INTEGER, new TypeOperators()))),
            TypeAndName.of("ab_test_v3_variant_ids", new ArrayType(IntegerType.INTEGER)),
            TypeAndName.of("app_id", VarcharType.VARCHAR));

    public static void writeActionBase(BlockBuilder builder, ActionBase base)
    {
        writeString(builder, base.getDeviceToken());
        writeString(builder, base.getVirtualId());
        writeString(builder, base.getEdition());
        writeString(builder, base.getPlatform());
        writeString(builder, base.getSessionId());
        writeString(builder, base.getAppVersion());
        writeString(builder, base.getSystemVersion());
        writeString(builder, base.getConnectionType());
        writeString(builder, base.getLocale());
        writeString(builder, base.getTimezone());
        writeString(builder, base.getHardware());

        writeLong(builder, base.getEventTimestamp());
        writeString(builder, base.getAccountId());
        writeLong(builder, base.getUserCreationTimestamp());
        List<Map<String, Integer>> abTest = base.getAbtestAsArray();
        if (abTest != null) {
            BlockBuilder arrayBuilder = builder.beginBlockEntry();
            for (Map<String, Integer> m : abTest) {
                BlockBuilder mapBuilder = arrayBuilder.beginBlockEntry();
                for (Map.Entry<String, Integer> entry : m.entrySet()) {
                    writeString(mapBuilder, entry.getKey());
                    writeInt(mapBuilder, entry.getValue());
                }
                arrayBuilder.closeEntry();
            }
            builder.closeEntry();
        }
        else {
            builder.appendNull();
        }

        if (base.getAbtestV3VariantIds() != null) {
            BlockBuilder arrayBuilder = builder.beginBlockEntry();
            for (Integer i : base.getAbtestV3VariantIds()) {
                writeInt(arrayBuilder, i);
            }
            builder.closeEntry();
        }
        else {
            builder.appendNull();
        }

        writeString(builder, base.getAppId());
    }

    public static void writeString(BlockBuilder builder, Object value)
    {
        if (value == null) {
            builder.appendNull();
        }
        else {
            VarcharType.VARCHAR.writeString(builder, (String) value);
        }
    }

    public static void writeLong(BlockBuilder builder, Object value)
    {
        if (value == null) {
            builder.appendNull();
        }
        else {
            BigintType.BIGINT.writeLong(builder, (long) value);
        }
    }

    public static void writeInt(BlockBuilder builder, Object value)
    {
        if (value == null) {
            builder.appendNull();
        }
        else {
            IntegerType.INTEGER.writeLong(builder, (int) value);
        }
    }

    public static void writeDouble(BlockBuilder builder, Object value)
    {
        if (value == null) {
            builder.appendNull();
        }
        else {
            DoubleType.DOUBLE.writeDouble(builder, (double) value);
        }
    }

    public static void writeMapStringString(BlockBuilder builder, Map<String, String> map)
    {
        if (map == null) {
            builder.appendNull();
        }
        else {
            BlockBuilder mapBuilder = builder.beginBlockEntry();
            for (Map.Entry<String, String> entry : map.entrySet()) {
                Utils.writeString(mapBuilder, entry.getKey());
                Utils.writeString(mapBuilder, entry.getValue());
            }
            builder.closeEntry();
        }
    }

    public static Integer[] parseIntArraySafe(String s, String delimiter, Integer expectedSize)
    {
        if (s == null) {
            return null;
        }
        String[] strArr = s.split(delimiter);
        if (expectedSize != null && strArr.length != expectedSize) {
            return null;
        }
        Integer[] intArr;
        try {
            intArr = new Integer[strArr.length];
            for (int i = 0; i < strArr.length; i++) {
                intArr[i] = Integer.parseInt(strArr[i]);
            }
        }
        catch (NumberFormatException e) {
            intArr = null;
        }
        return intArr;
    }

    public static Integer parseIntSafe(String s)
    {
        Integer num = null;
        try {
            num = Integer.parseInt(s);
        }
        catch (NumberFormatException ignored) {
        }
        return num;
    }
}
