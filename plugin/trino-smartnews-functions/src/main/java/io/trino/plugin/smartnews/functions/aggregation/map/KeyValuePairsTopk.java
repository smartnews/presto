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

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.CharType;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.openjdk.jol.info.ClassLayout;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class KeyValuePairsTopk
{
    private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(KeyValuePairsTopk.class).instanceSize());
    private static final int EXPECTED_ENTRIES = 50;
    private static final int EXPECTED_ENTRY_SIZE = 16;

    protected static final int EMPTY_SLOT = -1;

    protected final Type keyType;
    protected final Type valueType;
    protected final MethodHandle keyEqualOperator;
    protected final MethodHandle valueCompareOperator;

    protected final ArrayList<Block> keys;
    protected final ArrayList<Block> values;

    protected final int maxSize;

    public KeyValuePairsTopk(
            Type keyType,
            Type valueType,
            MethodHandle keyEqualOperator,
            MethodHandle valueCompareOperator,
            int maxSize)
    {
        this.keyType = requireNonNull(keyType, "keyType is null");
        this.valueType = requireNonNull(valueType, "valueType is null");
        this.keyEqualOperator = requireNonNull(keyEqualOperator, "keyEqualOperator is null");
        this.valueCompareOperator = requireNonNull(valueCompareOperator, "valueCompareOperator is null");
        this.values = new ArrayList<>(EXPECTED_ENTRIES);
        this.keys = new ArrayList<>(EXPECTED_ENTRIES);
        this.maxSize = maxSize;
    }

    public KeyValuePairsTopk(
            Block serialized,
            Type keyType,
            Type valueType,
            MethodHandle keyEqualOperator,
            MethodHandle valueCompareOperator,
            int maxSize)
    {
        this(keyType, valueType, keyEqualOperator, valueCompareOperator, maxSize);
        deserialize(requireNonNull(serialized, "serialized is null"));
    }

    // for copying
    protected KeyValuePairsTopk(
            Type keyType,
            Type valueType,
            MethodHandle keyEqualOperator,
            MethodHandle valueCompareOperator,
            ArrayList<Block> keys,
            ArrayList<Block> values,
            int maxSize)
    {
        this.keyType = keyType;
        this.valueType = valueType;
        this.keyEqualOperator = keyEqualOperator;
        this.valueCompareOperator = valueCompareOperator;
        this.keys = new ArrayList<>(keys.size() > 0 ? keys.size() : EXPECTED_ENTRIES);
        this.keys.addAll(keys);
        this.values = new ArrayList<>(values.size() > 0 ? values.size() : EXPECTED_ENTRIES);
        this.values.addAll(values);
        this.maxSize = maxSize;
    }

    public ArrayList<Block> getKeys()
    {
        return keys;
    }

    public ArrayList<Block> getValues()
    {
        return values;
    }

    protected void deserialize(Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            add(block, i, block, i + 1);
        }
    }

    public void serialize(BlockBuilder out)
    {
        BlockBuilder mapBlockBuilder = out.beginBlockEntry();
        for (int i = 0; i < keys.size(); i++) {
            keyType.appendTo(keys.get(i), 0, mapBlockBuilder);
            if (values.get(i) == null) {
                mapBlockBuilder.appendNull();
            }
            else {
                valueType.appendTo(values.get(i), 0, mapBlockBuilder);
            }
        }
        out.closeEntry();
    }

    public long estimatedInMemorySize()
    {
        long size = INSTANCE_SIZE;
        size += expectedValueSize(valueType, EXPECTED_ENTRY_SIZE) * values.size();
        size += expectedValueSize(keyType, EXPECTED_ENTRY_SIZE) * keys.size();
        return size;
    }

    /**
     * Only add this key value pair if we haven't seen this key before.
     * Otherwise, ignore it.
     */
    public void add(Block key, int keyPosition, Block value, int valuePosition)
    {
        add(key.getSingleValueBlock(keyPosition), value.getSingleValueBlock(valuePosition));
    }

    public void add(ArrayList<Block> keyArray, int keyPosition, ArrayList<Block> valueArray, int valuePosition)
    {
        add(keyArray.get(keyPosition), valueArray.get(valuePosition));
    }

    public void add(Block key, Block value)
    {
        int keyIndex = keyIndex(key);
        if (keyIndex == EMPTY_SLOT) {
            if (keys.size() >= maxSize) {
                int greaterIdx = valueIndexGreaterThan(value);
                if (greaterIdx != EMPTY_SLOT) {
                    keys.set(greaterIdx, key);
                    values.set(greaterIdx, value);
                }
            }
            else {
                keys.add(key);
                values.add(value);
            }
        }
        else {
            if (valueSmallerThanNullSafe(values.get(keyIndex), value)) {
                values.set(keyIndex, value);
            }
        }
    }

    protected int keyIndex(Block key)
    {
        for (int i = 0; i < keys.size(); i++) {
            if (keyEqualNullSafe(keys.get(i), key)) {
                return i;
            }
        }

        return EMPTY_SLOT;
    }

    protected int valueIndexGreaterThan(Block value)
    {
        for (int i = 0; i < values.size(); i++) {
            if (valueSmallerThanNullSafe(values.get(i), value)) {
                return i;
            }
        }

        return EMPTY_SLOT;
    }

    private boolean keyEqualNullSafe(Block key1, Block key2)
    {
        boolean leftIsNull = key1 == null;
        boolean rightIsNull = key2 == null;
        if (leftIsNull || rightIsNull) {
            return leftIsNull && rightIsNull;
        }
        try {
            return (Boolean) keyEqualOperator.invoke(key1, 0, key2, 0);
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private boolean valueSmallerThanNullSafe(Block val1, Block val2)
    {
        try {
            return (Long) valueCompareOperator.invokeExact(val1, (int) 0, val2, (int) 0) < 0;
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public KeyValuePairsTopk copy()
    {
        return new KeyValuePairsTopk(
                keyType,
                valueType,
                keyEqualOperator,
                valueCompareOperator,
                keys,
                values,
                maxSize);
    }

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

    public int getMaxSize()
    {
        return maxSize;
    }
}
