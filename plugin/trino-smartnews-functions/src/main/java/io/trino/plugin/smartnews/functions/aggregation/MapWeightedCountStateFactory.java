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

import io.airlift.slice.Slice;
import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.GroupedAccumulatorState;
import org.openjdk.jol.info.ClassLayout;

import java.util.Map;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class MapWeightedCountStateFactory
        implements AccumulatorStateFactory<MapString2DoubleState>
{
    @Override
    public MapString2DoubleState createSingleState()
    {
        return new SingleMapString2DoubleState();
    }

    @Override
    public MapString2DoubleState createGroupedState()
    {
        return new GroupedMapString2DoubleState();
    }

    public static class GroupedMapString2DoubleState
            implements GroupedAccumulatorState, MapString2DoubleState
    {
        private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(MapString2DoubleState.class).instanceSize());
        private final ObjectBigArray<Map<Slice, Double>> maps = new ObjectBigArray<>();
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
        public Map<Slice, Double> getMap()
        {
            return maps.get(groupId);
        }

        @Override
        public void setMap(Map<Slice, Double> value)
        {
            requireNonNull(value, "value is null");
            maps.set(groupId, value);
        }

        @Override
        public void addMemoryUsage(int value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            long ret = INSTANCE_SIZE * size + maps.sizeOf();
            return ret;
        }
    }

    public static class SingleMapString2DoubleState
            implements MapString2DoubleState
    {
        private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(SingleMapString2DoubleState.class).instanceSize());
        private Map<Slice, Double> map;

        @Override
        public Map<Slice, Double> getMap()
        {
            return map;
        }

        @Override
        public void setMap(Map<Slice, Double> value)
        {
            map = value;
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
            if (map != null) {
                estimatedSize += map.size() * (Double.BYTES + 14);
            }
            return estimatedSize;
        }
    }
}
