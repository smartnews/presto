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
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;

public class KeyValuePairsSum
        extends KeyValuePairsBase
{
    public KeyValuePairsSum(
            Type keyType,
            MethodHandle keyEqualOperator,
            MethodHandle keyHashCodeOperator,
            Type valueType)
    {
        super(keyType, keyEqualOperator, keyHashCodeOperator, valueType);
    }

    public KeyValuePairsSum(
            Block serialized,
            Type keyType,
            MethodHandle keyEqualOperator,
            MethodHandle keyHashCodeOperator,
            Type valueType)

    {
        super(serialized, keyType, keyEqualOperator, keyHashCodeOperator, valueType);
    }

    // for copying
    protected KeyValuePairsSum(
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
        super(keyBlockBuilder, keyType, keyEqualOperator, keyHashCodeOperator,
                values, valueType, keyPositionByHash, hashCapacity, maxFill, hashMask);
    }

    @Override
    protected Object reduce(Object a, Object b)
    {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        if (valueType instanceof BigintType) {
            return ((Long) a) + ((Long) b);
        }
        else if (valueType instanceof DoubleType) {
            return ((Double) a) + ((Double) b);
        }
        else if (valueType instanceof IntegerType) {
            return ((Integer) a) + ((Integer) b);
        }
        else if (valueType instanceof RealType) {
            return ((Float) a) + ((Float) b);
        }
        else if (valueType instanceof BooleanType) {
            return ((Boolean) a) || ((Boolean) b);
        }
        else {
            throw new RuntimeException("Unsupported type: " + valueType);
        }
    }

    @Override
    public KeyValuePairsSum copy()
    {
        BlockBuilder keyBlockBuilderCopy = null;
        if (keyBlockBuilder != null) {
            keyBlockBuilderCopy = (BlockBuilder) keyBlockBuilder.copyRegion(0, keyBlockBuilder.getPositionCount());
        }

        return new KeyValuePairsSum(
                keyBlockBuilderCopy,
                keyType,
                keyEqualOperator,
                keyHashCodeOperator,
                values,
                valueType,
                keyPositionByHash.clone(),
                hashCapacity,
                maxFill,
                hashMask);
    }
}
