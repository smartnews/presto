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
package io.trino.plugin.smartnews.functions.aggregation.videoview;

import io.trino.array.ObjectBigArray;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.AccumulatorStateMetadata;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.function.GroupedAccumulatorState;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;

import java.util.BitSet;

@AccumulatorStateMetadata(stateSerializerClass = BitSetState.BitSetStateSerializer.class, stateFactoryClass = BitSetState.BitSetStateStateFactory.class)
public interface BitSetState
        extends AccumulatorState
{
    java.util.BitSet getBitSet();

    void setBitSet(java.util.BitSet value);

    void addMemoryUsage(int value);

    static void serialize(BitSetState state, BlockBuilder out)
    {
        BitSet bitSet = state.getBitSet();
        if (bitSet == null) {
            out.appendNull();
        }
        else {
            long[] words = bitSet.toLongArray();
            BlockBuilder blockBuilder = out.beginBlockEntry();
            for (long word : words) {
                BigintType.BIGINT.writeLong(blockBuilder, word);
            }
            out.closeEntry();
        }
    }

    class BitSetStateSerializer
            implements AccumulatorStateSerializer<BitSetState>
    {
        Type type = new ArrayType(BigintType.BIGINT);

        @Override
        public Type getSerializedType()
        {
            return type;
        }

        @Override
        public void serialize(BitSetState state, BlockBuilder out)
        {
            BitSetState.serialize(state, out);
        }

        @Override
        public void deserialize(Block block, int index, BitSetState state)
        {
            if (!block.isNull(index)) {
                Block arrayBlock = (Block) type.getObject(block, index);
                long[] words = new long[arrayBlock.getPositionCount()];
                for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                    words[i] = BigintType.BIGINT.getLong(arrayBlock, i);
                }
                java.util.BitSet bitSet = java.util.BitSet.valueOf(words);
                state.setBitSet(bitSet);
            }
        }
    }

    class BitSetStateStateFactory
            implements AccumulatorStateFactory<BitSetState>
    {
        @Override
        public BitSetState createSingleState()
        {
            return new SingleBitSetState();
        }

        @Override
        public BitSetState createGroupedState()
        {
            return new GroupedBitSetState();
        }
    }

    class SingleBitSetState
            implements BitSetState
    {
        private java.util.BitSet bitSet;
        private int memoryUsage;

        @Override
        public java.util.BitSet getBitSet()
        {
            return bitSet;
        }

        @Override
        public void setBitSet(java.util.BitSet value)
        {
            this.bitSet = value;
        }

        @Override
        public void addMemoryUsage(int value)
        {
            this.memoryUsage += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return memoryUsage;
        }
    }

    class GroupedBitSetState
            implements GroupedAccumulatorState, BitSetState
    {
        private final ObjectBigArray<BitSet> maps = new ObjectBigArray<>();
        private long size;
        private long groupId;

        @Override
        public void setGroupId(long groupId)
        {
            this.groupId = groupId;
        }

        @Override
        public void ensureCapacity(long size)
        {
            maps.ensureCapacity(size);
        }

        @Override
        public long getEstimatedSize()
        {
            return 0;
        }

        @Override
        public BitSet getBitSet()
        {
            return maps.get(groupId);
        }

        @Override
        public void setBitSet(BitSet value)
        {
            maps.set(groupId, value);
        }

        @Override
        public void addMemoryUsage(int value)
        {
        }
    }
}
