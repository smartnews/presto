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
package com.facebook.presto.smartnews.functions.aggregation;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class ArbitraryN
{
    private int capacity;
    private int positionCount;
    private BlockBuilder blockBuilder;
    private final Type type = VARCHAR;
    private final Type outputType = new ArrayType(VARCHAR);

    public ArbitraryN(int capacity)
    {
        this.capacity = capacity;
        this.blockBuilder = type.createBlockBuilder(null, capacity);
    }

    public boolean isEmpty()
    {
        return positionCount == 0;
    }

    public boolean isFull()
    {
        return positionCount == capacity;
    }

    public void add(Slice slice)
    {
        if (!isFull()) {
            this.positionCount++;
            type.writeSlice(blockBuilder, slice);
        }
    }

    public void add(BlockBuilder otherBlockBuilder, int i)
    {
        if (!isFull()) {
            this.positionCount++;
            type.appendTo(otherBlockBuilder, i, blockBuilder);
        }
    }

    private void add(Block otherBlock, int i)
    {
        if (!isFull()) {
            this.positionCount++;
            type.appendTo(otherBlock, i, blockBuilder);
        }
    }

    public void addAll(Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (!isFull()) {
                add(block, i);
            }
        }
    }

    public BlockBuilder getBlockBuilder()
    {
        return blockBuilder;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public int getCapacity()
    {
        return capacity;
    }

    public void writeAll(BlockBuilder outputBlockBuilder)
    {
        for (int i = 0; i < positionCount; i++) {
            type.appendTo(blockBuilder, i, outputBlockBuilder);
        }
    }

    public void writeAsArray(BlockBuilder outputBlockBuilder)
    {
        BlockBuilder elements = outputBlockBuilder.beginBlockEntry();
        writeAll(elements);
        outputBlockBuilder.closeEntry();
    }
}
