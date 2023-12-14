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
package io.trino.plugin.smartnews.functions.aggregation;

import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.GroupedAccumulatorState;
import org.openjdk.jol.info.ClassLayout;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class SnHyperLogLogStateFactory
        implements AccumulatorStateFactory<SnHyperLogLogState>
{
    @Override
    public SnHyperLogLogState createSingleState()
    {
        return new SingleHyperLogLogState();
    }

    @Override
    public SnHyperLogLogState createGroupedState()
    {
        return new GroupedHyperLogLogState();
    }

    public static class GroupedHyperLogLogState
            implements GroupedAccumulatorState, SnHyperLogLogState
    {
        private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(GroupedHyperLogLogState.class).instanceSize());
        private final ObjectBigArray<com.smartnews.dmp.core.data.HyperLogLog> hlls = new ObjectBigArray<>();
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
            hlls.ensureCapacity(size);
        }

        @Override
        public com.smartnews.dmp.core.data.HyperLogLog getHyperLogLog()
        {
            return hlls.get(groupId);
        }

        @Override
        public void setHyperLogLog(com.smartnews.dmp.core.data.HyperLogLog value)
        {
            requireNonNull(value, "value is null");
            hlls.set(groupId, value);
        }

        @Override
        public void addMemoryUsage(int value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + hlls.sizeOf();
        }
    }

    public static class SingleHyperLogLogState
            implements SnHyperLogLogState
    {
        private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(SingleHyperLogLogState.class).instanceSize());
        private com.smartnews.dmp.core.data.HyperLogLog hll;

        @Override
        public com.smartnews.dmp.core.data.HyperLogLog getHyperLogLog()
        {
            return hll;
        }

        @Override
        public void setHyperLogLog(com.smartnews.dmp.core.data.HyperLogLog value)
        {
            hll = value;
        }

        @Override
        public void addMemoryUsage(int value)
        {
            // noop
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (hll != null) {
                //estimatedSize += hll.estimatedInMemorySize();
            }
            return estimatedSize;
        }
    }
}
