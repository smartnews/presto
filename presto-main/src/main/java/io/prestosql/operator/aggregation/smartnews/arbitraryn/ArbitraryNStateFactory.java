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
package io.prestosql.operator.aggregation.smartnews.arbitraryn;

import io.prestosql.array.ObjectBigArray;
import io.prestosql.spi.function.AccumulatorStateFactory;
import io.prestosql.spi.function.GroupedAccumulatorState;
import io.prestosql.spi.type.Type;

public class ArbitraryNStateFactory
        implements AccumulatorStateFactory<ArbitraryNState>
{
    private final Type type;

    public ArbitraryNStateFactory(Type type)
    {
        this.type = type;
    }

    @Override
    public ArbitraryNState createSingleState()
    {
        return new SingleArbitraryNState();
    }

    @Override
    public Class<? extends ArbitraryNState> getSingleStateClass()
    {
        return SingleArbitraryNState.class;
    }

    @Override
    public ArbitraryNState createGroupedState()
    {
        return new GroupedArbitraryNState();
    }

    @Override
    public Class<? extends ArbitraryNState> getGroupedStateClass()
    {
        return GroupedArbitraryNState.class;
    }

    public static class SingleArbitraryNState
            implements ArbitraryNState
    {
        private ArbitraryN data;
        private long size;

        @Override
        public ArbitraryN getData()
        {
            return data;
        }

        @Override
        public void setData(ArbitraryN data)
        {
            this.data = data;
        }

        @Override
        public void addMemoryUsage(long memory)
        {
            size += memory;
        }

        @Override
        public long getEstimatedSize()
        {
            return size;
        }
    }

    public static class GroupedArbitraryNState
            implements GroupedAccumulatorState, ArbitraryNState
    {
        private final ObjectBigArray<ArbitraryN> dataArray = new ObjectBigArray<>();
        private long groupId;
        private long size;

        @Override
        public ArbitraryN getData()
        {
            return dataArray.get(groupId);
        }

        @Override
        public void setData(ArbitraryN data)
        {
            dataArray.set(groupId, data);
        }

        @Override
        public void addMemoryUsage(long memory)
        {
            size += memory;
        }

        @Override
        public long getEstimatedSize()
        {
            return size + dataArray.sizeOf();
        }

        @Override
        public void setGroupId(long groupId)
        {
            this.groupId = groupId;
        }

        @Override
        public void ensureCapacity(long size)
        {
            dataArray.ensureCapacity(size);
        }
    }
}
