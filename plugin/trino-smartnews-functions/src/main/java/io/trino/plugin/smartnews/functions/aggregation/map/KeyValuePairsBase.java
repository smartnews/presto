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
package io.trino.plugin.smartnews.functions.aggregation.map;

import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.openjdk.jol.info.ClassLayout;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public abstract class KeyValuePairsBase
{
    private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(KeyValuePairsBase.class).instanceSize());
    private static final int EXPECTED_ENTRIES = 10;
    private static final int EXPECTED_ENTRY_SIZE = 16;
    private static final float FILL_RATIO = 0.75f;
    protected static final int EMPTY_SLOT = -1;

    protected final BlockBuilder keyBlockBuilder;
    protected final Type keyType;
    protected final MethodHandle keyEqualOperator;
    protected final MethodHandle keyHashCodeOperator;

    protected final ArrayList<Object> values;
    protected final Type valueType;

    protected int[] keyPositionByHash;
    protected int hashCapacity;
    protected int maxFill;
    protected int hashMask;

    public KeyValuePairsBase(
            Type keyType,
            MethodHandle keyEqualOperator,
            MethodHandle keyHashCodeOperator,
            Type valueType)
    {
        this.keyType = requireNonNull(keyType, "keyType is null");
        this.valueType = requireNonNull(valueType, "valueType is null");
        this.keyEqualOperator = requireNonNull(keyEqualOperator, "keyEqualOperator is null");
        this.keyHashCodeOperator = requireNonNull(keyHashCodeOperator, "keyHashCodeOperator is null");
        keyBlockBuilder = this.keyType.createBlockBuilder(null, EXPECTED_ENTRIES, expectedValueSize(keyType, EXPECTED_ENTRY_SIZE));
        hashCapacity = arraySize(EXPECTED_ENTRIES, FILL_RATIO);
        this.maxFill = calculateMaxFill(hashCapacity);
        this.hashMask = hashCapacity - 1;
        keyPositionByHash = new int[hashCapacity];
        Arrays.fill(keyPositionByHash, EMPTY_SLOT);
        this.values = new ArrayList<>(EXPECTED_ENTRIES);
    }

    public KeyValuePairsBase(
            Block serialized,
            Type keyType,
            MethodHandle keyEqualOperator,
            MethodHandle keyHashCodeOperator,
            Type valueType)
    {
        this(keyType, keyEqualOperator, keyHashCodeOperator, valueType);
        deserialize(requireNonNull(serialized, "serialized is null"));
    }

    // for copying
    protected KeyValuePairsBase(
            BlockBuilder keyBlockBuilder,
            Type keyType,
            MethodHandle keyEqualOperator,
            MethodHandle keyHashCodeOperator,
            ArrayList<Object> values,
            Type valueType,
            int[] keyPositionByHash,
            int hashCapacity,
            int maxFill,
            int hashMask)
    {
        this.keyBlockBuilder = keyBlockBuilder;
        this.keyType = keyType;
        this.keyEqualOperator = keyEqualOperator;
        this.keyHashCodeOperator = keyHashCodeOperator;
        this.valueType = valueType;
        this.keyPositionByHash = keyPositionByHash;
        this.hashCapacity = hashCapacity;
        this.maxFill = maxFill;
        this.hashMask = hashMask;
        this.values = new ArrayList<>(values.size());
        this.values.addAll(values);
    }

    public Block getKeys()
    {
        return keyBlockBuilder.build();
    }

    protected void deserialize(Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            add(block, block, i, i + 1);
        }
    }

    public void serialize(BlockBuilder out)
    {
        BlockBuilder mapBlockBuilder = out.beginBlockEntry();
        for (int i = 0; i < keyBlockBuilder.getPositionCount(); i++) {
            keyType.appendTo(keyBlockBuilder, i, mapBlockBuilder);
            if (values.get(i) == null) {
                mapBlockBuilder.appendNull();
            }
            else {
                serialize(mapBlockBuilder, values.get(i));
            }
        }
        out.closeEntry();
    }

    void serialize(BlockBuilder out, Object o)
    {
        if (valueType instanceof BigintType) {
            valueType.writeLong(out, (long) o);
        }
        else if (valueType instanceof DoubleType) {
            valueType.writeDouble(out, (double) o);
        }
        else if (valueType instanceof IntegerType) {
            valueType.writeLong(out, (int) o);
        }
        else if (valueType instanceof RealType) {
            int intVal = Float.floatToIntBits((float) o);
            valueType.writeLong(out, intVal);
        }
        else if (valueType instanceof BooleanType) {
            valueType.writeBoolean(out, (boolean) o);
        }
        else {
            throw new RuntimeException("Unsupported type: " + valueType);
        }
    }

    public long estimatedInMemorySize()
    {
        long size = INSTANCE_SIZE;
        size += keyBlockBuilder.getRetainedSizeInBytes();
        //size += valueBlockBuilder.getRetainedSizeInBytes();
        size += expectedValueSize(valueType, EXPECTED_ENTRY_SIZE) * values.size();
        size += sizeOf(keyPositionByHash);
        return size;
    }

    /**
     * Only add this key value pair if we haven't seen this key before.
     * Otherwise, ignore it.
     */
    public void add(Block key, Block value, int keyPosition, int valuePosition)
    {
        int keyIndex = keyIndex(key, keyPosition);
        if (keyIndex == EMPTY_SLOT) {
            addKey(key, keyPosition);
            if (value.isNull(valuePosition)) {
                values.add(null);
            }
            else {
                values.add(valueType.getObjectValue(null, value, valuePosition));
            }
        }
        else {
            if (!value.isNull(valuePosition)) {
                Object obj = valueType.getObjectValue(null, value, valuePosition);
                values.set(keyIndex, reduce(obj, values.get(keyIndex)));
            }
        }
    }

    public void add(Block key, ArrayList<Object> valueArray, int keyPosition, int valuePosition)
    {
        int keyIndex = keyIndex(key, keyPosition);
        if (keyIndex == EMPTY_SLOT) {
            addKey(key, keyPosition);
            values.add(valueArray.get(valuePosition));
        }
        else {
            Object obj = valueArray.get(valuePosition);
            if (obj != null) {
                values.set(keyIndex, reduce(obj, values.get(keyIndex)));
            }
        }
    }

    protected abstract Object reduce(Object a, Object b);

    protected int keyIndex(Block key, int position)
    {
        checkArgument(position >= 0, "position is negative");
        return keyPositionByHash[getHashPositionOfKey(key, position)];
    }

    protected int addKey(Block key, int position)
    {
        checkArgument(position >= 0, "position is negative");
        keyType.appendTo(key, position, keyBlockBuilder);
        int hashPosition = getHashPositionOfKey(key, position);
        if (keyPositionByHash[hashPosition] == EMPTY_SLOT) {
            keyPositionByHash[hashPosition] = keyBlockBuilder.getPositionCount() - 1;
            if (keyBlockBuilder.getPositionCount() >= maxFill) {
                rehash();
            }
        }
        return keyPositionByHash[hashPosition];
    }

    private long hashCode(Block key, int position)
    {
        if (key.isNull(position)) {
            return 0;
        }
        long hashCode = 0;
        try {
            hashCode = (long) keyHashCodeOperator.invokeExact(key, position);
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
        return (int) hashCode;
    }

    private boolean equalNullSafe(int hashPosition, Block key, int position)
    {
        int existingKeyPosition = keyPositionByHash[hashPosition];
        boolean leftIsNull = keyBlockBuilder.isNull(existingKeyPosition);
        boolean rightIsNull = key.isNull(position);
        if (leftIsNull || rightIsNull) {
            return leftIsNull && rightIsNull;
        }
        try {
            return (Boolean) keyEqualOperator.invokeExact((Block) keyBlockBuilder, existingKeyPosition, key, position);
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private int getHashPositionOfKey(Block key, int position)
    {
        int hashPosition = getMaskedHash(hashCode(key, position));
        while (true) {
            if (keyPositionByHash[hashPosition] == EMPTY_SLOT) {
                return hashPosition;
            }
            if (equalNullSafe(hashPosition, key, position)) {
                return hashPosition;
            }
            hashPosition = getMaskedHash(hashPosition + 1);
        }
    }

    private void rehash()
    {
        long newCapacityLong = hashCapacity * 2L;
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new TrinoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = (int) newCapacityLong;
        hashCapacity = newCapacity;
        hashMask = newCapacity - 1;
        maxFill = calculateMaxFill(newCapacity);
        keyPositionByHash = new int[newCapacity];
        Arrays.fill(keyPositionByHash, EMPTY_SLOT);
        for (int position = 0; position < keyBlockBuilder.getPositionCount(); position++) {
            keyPositionByHash[getHashPositionOfKey(keyBlockBuilder, position)] = position;
        }
    }

    private static int calculateMaxFill(int hashSize)
    {
        checkArgument(hashSize > 0, "hashSize must be greater than 0");
        int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
        if (maxFill == hashSize) {
            maxFill--;
        }
        checkArgument(hashSize > maxFill, "hashSize must be larger than maxFill");
        return maxFill;
    }

    private int getMaskedHash(long rawHash)
    {
        return (int) (rawHash & hashMask);
    }

    public abstract KeyValuePairsBase copy();

    public static int expectedValueSize(Type type, int defaultSize)
    {
        if (type instanceof FixedWidthType) {
            return ((FixedWidthType) type).getFixedSize();
        }
        // If bound on length of varchar or char is smaller than defaultSize, use that as expected size
        // The data can take up to 4 bytes per character due to UTF-8 encoding, but we assume it is ASCII and only needs one byte.
        if (type instanceof VarcharType) {
            return ((VarcharType) type).getLength()
                    .map(length -> Math.min(length, defaultSize))
                    .orElse(defaultSize);
        }
        if (type instanceof CharType) {
            return Math.min(((CharType) type).getLength(), defaultSize);
        }
        return defaultSize;
    }
}
