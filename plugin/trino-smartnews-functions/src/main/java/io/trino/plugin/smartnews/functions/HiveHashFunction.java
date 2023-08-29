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
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.SingleMapBlock;
import io.trino.spi.block.SingleRowBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalConversions;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeUtils;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;

@ScalarFunction("hash")
@Description("Returns int hash for any type of argument")
public final class HiveHashFunction
{
    private HiveHashFunction() {}

    @TypeParameter("T")
    @SqlType(StandardTypes.BIGINT)
    public static long hashFunc(@TypeParameter("T") Type type, @SqlNullable @SqlType("T") Object value)
    {
        return hash(type, value);
    }

    public static int hash(Type type, Object value)
    {
        if (value == null) {
            return 0;
        }
        else if (type instanceof VarbinaryType && value instanceof Slice slice) {
            byte[] bytes = slice.getBytes();
            return hashBytesHiveBinary(bytes, 0, bytes.length);
        }
        else if (type instanceof TimestampType timestampType) {
            if (timestampType.isShort()) {
                long ts = (long) value;
                long seconds = ts / 1000000;
                long nanos = (ts % 1000000) * 1000;

                seconds <<= 30;  // the nanosecond part fits in 30 bits
                seconds |= nanos;
                return (int) ((seconds >>> 32) ^ seconds);
            }
            else if (value instanceof LongTimestamp longTimestamp) {
                long micros = longTimestamp.getEpochMicros();
                int picos = longTimestamp.getPicosOfMicro();
                long seconds = micros / 1000000;
                long nanos = (micros % 1000000) * 1000 + picos / 1000;

                seconds <<= 30;  // the nanosecond part fits in 30 bits
                seconds |= nanos;
                return (int) ((seconds >>> 32) ^ seconds);
            }
        }
        else if (value instanceof Boolean booleanValue) {
            return booleanValue ? 1 : 0;
        }
        else if (value instanceof Byte byteValue) {
            return byteValue;
        }
        else if (value instanceof Short shortValue) {
            return shortValue;
        }
        else if (value instanceof Integer intValue) {
            return intValue;
        }
        else if (type instanceof DecimalType decimalType) {
            return BigDecimal.valueOf((long) value, DecimalConversions.intScale(decimalType.getScale())).hashCode();
        }
        else if (value instanceof Long longValue) {
            return (int) ((longValue >>> 32) ^ longValue);
        }
        else if (value instanceof Float floatValue) {
            return Float.floatToIntBits(floatValue);
        }
        else if (value instanceof Double doubleValue) {
            long longValue = Double.doubleToLongBits(doubleValue);
            return (int) ((longValue >>> 32) ^ longValue);
        }
        else if (type instanceof VarcharType && value instanceof String stringValue) {
            byte[] bytes = stringValue.getBytes(StandardCharsets.UTF_8);
            int r = 0;
            for (int i = 0; i < bytes.length; i++) {
                r = r * 31 + bytes[i];
            }
            return r;
        }
        else if (value instanceof Slice stringSlice) {
            String stringValue = stringSlice.toStringUtf8();
            if (stringValue == null) {
                return 0;
            }
            byte[] bytes = stringValue.getBytes(StandardCharsets.UTF_8);
            int r = 0;
            for (int i = 0; i < bytes.length; i++) {
                r = r * 31 + bytes[i];
            }
            return r;
        }
        else if (value instanceof SingleMapBlock mapBlock) {
            //Type type = mapBlock.getMapType();
            if (type instanceof MapType mapType) {
                Type keyType = mapType.getKeyType();
                Type valueType = mapType.getValueType();
                int cnt = mapBlock.getPositionCount();
                int r = 0;
                for (int i = 0; i < cnt; i += 2) {
                    Object key = TypeUtils.readNativeValue(keyType, mapBlock, i);
                    Object val = TypeUtils.readNativeValue(valueType, mapBlock, i + 1);

                    int keyHash = hash(keyType, key);
                    int valHash = hash(valueType, val);
                    r += keyHash ^ valHash;
                }
                return r;
            }
            return mapBlock.hashCode();
        }
        else if (value instanceof VariableWidthBlock charArrayBlock) {
            ArrayType arrayType = (ArrayType) type;
            int cnt = charArrayBlock.getPositionCount();
            int r = 0;
            for (int i = 0; i < cnt; i++) {
                String str = null;
                if (!charArrayBlock.isNull(i)) {
                    int len = charArrayBlock.getSliceLength(i);
                    str = charArrayBlock.getSlice(i, 0, len).toStringUtf8();
                }
                r = r * 31 + hash(arrayType.getElementType(), str);   // TODO: check type
            }
            return r;
        }
        else if (value instanceof IntArrayBlock intArrayBlock) {
            int cnt = intArrayBlock.getPositionCount();
            int r = 0;
            for (int i = 0; i < cnt; i++) {
                if (!intArrayBlock.isNull(i)) {
                    r = r * 31 + intArrayBlock.getInt(i, 0);
                }
                else {
                    r = r * 31;
                }
            }
            return r;
        }
        else if (value instanceof LongArrayBlock longArrayBlock) {
            int cnt = longArrayBlock.getPositionCount();
            int r = 0;
            for (int i = 0; i < cnt; i++) {
                if (!longArrayBlock.isNull(i)) {
                    long longValue = longArrayBlock.getLong(i, 0);
                    r = r * 31 + (int) ((longValue >>> 32) ^ longValue);
                }
                else {
                    r = r * 31;
                }
            }
            return r;
        }
        else if (value instanceof ArrayBlock arrayBlock) {
            ArrayType arrayType = (ArrayType) type;
            int cnt = arrayBlock.getPositionCount();
            int r = 0;
            for (int i = 0; i < cnt; i++) {
                int hash = 0;
                if (!arrayBlock.isNull(i)) {
                    Object obj = TypeUtils.readNativeValue(arrayType.getElementType(), arrayBlock, i);
                    hash = hash(arrayType.getElementType(), obj);
                }
                r = r * 31 + hash;
            }
            return r;
        }
        else if (type instanceof ArrayType && value instanceof Block) {
            ArrayType arrayType = (ArrayType) type;
            Block arrayBlock = (Block) value;

            int cnt = arrayBlock.getPositionCount();
            int r = 0;
            for (int i = 0; i < cnt; i++) {
                int hash = 0;
                if (!arrayBlock.isNull(i)) {
                    Object obj = TypeUtils.readNativeValue(arrayType.getElementType(), arrayBlock, i);
                    hash = hash(arrayType.getElementType(), obj);
                    r = r * 31 + hash;
                }
            }
            return r;
        }
        else if (type instanceof RowType rowType && value instanceof SingleRowBlock rowBlock) {
            List<Type> types = rowType.getTypeParameters();
            int r = 0;
            for (int i = 0; i < types.size(); i++) {
                int hash = 0;
                if (!rowBlock.isNull(i)) {
                    Object obj = TypeUtils.readNativeValue(types.get(i), rowBlock, i);
                    hash = hash(types.get(i), obj);
                }
                r = r * 31 + hash;
            }
            return r;
        }

        return value == null ? 0 : value.hashCode();
    }

    public static int hashBytesHiveBinary(byte[] bytes, int offset, int length)
    {
        int hash = 1;

        for (int i = offset; i < offset + length; ++i) {
            hash = 31 * hash + bytes[i];
        }

        return hash;
    }
}
