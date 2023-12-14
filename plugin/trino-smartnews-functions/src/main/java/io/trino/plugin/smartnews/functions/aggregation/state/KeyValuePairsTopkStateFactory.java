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
package io.trino.plugin.smartnews.functions.aggregation.state;

import io.trino.array.ObjectBigArray;
import io.trino.plugin.smartnews.functions.aggregation.map.KeyValuePairsTopk;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class KeyValuePairsTopkStateFactory
        implements AccumulatorStateFactory<KeyValuePairsTopkState>
{
    private final Type keyType;
    private final Type valueType;

    public KeyValuePairsTopkStateFactory(@TypeParameter("K") Type keyType, @TypeParameter("V") Type valueType)
    {
        this.keyType = keyType;
        this.valueType = valueType;
    }

    @Override
    public KeyValuePairsTopkState createSingleState()
    {
        return new SingleState(keyType, valueType);
    }

    @Override
    public KeyValuePairsTopkState createGroupedState()
    {
        return new GroupedState(keyType, valueType);
    }

    public static class GroupedState
            extends AbstractGroupedAccumulatorState
            implements KeyValuePairsTopkState
    {
        private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(GroupedState.class).instanceSize());
        private final Type keyType;
        private final Type valueType;
        private final ObjectBigArray<KeyValuePairsTopk> pairs = new ObjectBigArray<>();
        private long size;

        public GroupedState(Type keyType, Type valueType)
        {
            this.keyType = keyType;
            this.valueType = valueType;
        }

        @Override
        public void ensureCapacity(long size)
        {
            pairs.ensureCapacity(size);
        }

        @Override
        public KeyValuePairsTopk get()
        {
            return pairs.get(getGroupId());
        }

        @Override
        public void set(KeyValuePairsTopk value)
        {
            requireNonNull(value, "value is null");

            KeyValuePairsTopk previous = get();
            if (previous != null) {
                size -= previous.estimatedInMemorySize();
            }

            pairs.set(getGroupId(), value);
            size += value.estimatedInMemorySize();
        }

        @Override
        public void addMemoryUsage(long memory)
        {
            size += memory;
        }

        @Override
        public Type getKeyType()
        {
            return keyType;
        }

        @Override
        public Type getValueType()
        {
            return valueType;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + pairs.sizeOf();
        }
    }

    public static class SingleState
            implements KeyValuePairsTopkState
    {
        private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(SingleState.class).instanceSize());
        private final Type keyType;
        private final Type valueType;
        private KeyValuePairsTopk pair;

        public SingleState(Type keyType, Type valueType)
        {
            this.keyType = keyType;
            this.valueType = valueType;
        }

        // for copying
        private SingleState(Type keyType, Type valueType, KeyValuePairsTopk pair)
        {
            this.keyType = keyType;
            this.valueType = valueType;
            this.pair = pair;
        }

        @Override
        public KeyValuePairsTopk get()
        {
            return pair;
        }

        @Override
        public void set(KeyValuePairsTopk value)
        {
            pair = value;
        }

        @Override
        public void addMemoryUsage(long memory)
        {
        }

        @Override
        public Type getKeyType()
        {
            return keyType;
        }

        @Override
        public Type getValueType()
        {
            return valueType;
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (pair != null) {
                estimatedSize += pair.estimatedInMemorySize();
            }
            return estimatedSize;
        }

        @Override
        public AccumulatorState copy()
        {
            KeyValuePairsTopk pairCopy = null;
            if (pair != null) {
                pairCopy = pair.copy();
            }
            return new SingleState(keyType, valueType, pairCopy);
        }
    }
}
