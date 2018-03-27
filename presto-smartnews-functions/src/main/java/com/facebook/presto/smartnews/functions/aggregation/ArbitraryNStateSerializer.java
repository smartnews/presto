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
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static java.lang.Math.toIntExact;

public class ArbitraryNStateSerializer
        implements AccumulatorStateSerializer<ArbitraryNState>
{
    private final Type serializedType;
    private final ArrayType arrayType;

    public ArbitraryNStateSerializer()
    {
        this.arrayType = new ArrayType(VarcharType.VARCHAR);
        this.serializedType = new RowType(ImmutableList.of(BIGINT, arrayType), Optional.empty());
    }

    @Override
    public Type getSerializedType()
    {
        return serializedType;
    }

    @Override
    public void serialize(ArbitraryNState state, BlockBuilder out)
    {
        ArbitraryN data = state.getData();
        if (data == null) {
            out.appendNull();
            return;
        }

        BlockBuilder blockBuilder = out.beginBlockEntry();

        BIGINT.writeLong(blockBuilder, data.getCapacity());
        BlockBuilder elements = blockBuilder.beginBlockEntry();
        data.writeAll(elements);
        blockBuilder.closeEntry();
        out.closeEntry();
    }

    @Override
    public void deserialize(Block block, int index, ArbitraryNState state)
    {
        Block currentBlock = (Block) serializedType.getObject(block, index);
        int capacity = toIntExact(BIGINT.getLong(currentBlock, 0));
        Block dataBlock = arrayType.getObject(currentBlock, 1);
        ArbitraryN data = new ArbitraryN(capacity);
        data.addAll(dataBlock);
        state.setData(data);
    }
}
