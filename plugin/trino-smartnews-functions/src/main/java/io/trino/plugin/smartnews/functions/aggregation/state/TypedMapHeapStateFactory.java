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
import io.trino.plugin.smartnews.functions.aggregation.map.TypedMapHeap;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.GroupedAccumulatorState;
import org.openjdk.jol.info.ClassLayout;

import java.util.function.Function;
import java.util.function.LongFunction;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.toIntExact;

public class TypedMapHeapStateFactory
{
    private abstract static class AbstractTypedMapHeapState
            implements TypedMapHeapState
    {
        abstract TypedMapHeap getTypedMapHeap();
    }

    public static class GroupedTypedMapHeapState
            extends AbstractTypedMapHeapState
            implements GroupedAccumulatorState
    {
        private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(GroupedTypedMapHeapState.class).instanceSize());

        private final LongFunction<TypedMapHeap> heapFactory;
        private final Function<Block, TypedMapHeap> deserializer;

        private final ObjectBigArray<TypedMapHeap> heaps = new ObjectBigArray<>();
        private long groupId;
        private long size;

        public GroupedTypedMapHeapState(LongFunction<TypedMapHeap> heapFactory, Function<Block, TypedMapHeap> deserializer)
        {
            this.heapFactory = heapFactory;
            this.deserializer = deserializer;
        }

        @Override
        public final void setGroupId(long groupId)
        {
            this.groupId = groupId;
        }

        @Override
        public final void ensureCapacity(long size)
        {
            heaps.ensureCapacity(size);
        }

        @Override
        public final long getEstimatedSize()
        {
            return INSTANCE_SIZE + heaps.sizeOf() + size;
        }

        @Override
        public final void initialize(long n)
        {
            if (getTypedMapHeap() == null) {
                TypedMapHeap typedHeap = heapFactory.apply(n);
                setTypedMapHeap(typedHeap);
                size += typedHeap.getEstimatedSize();
            }
        }

        @Override
        public final void add(Block keyBlock, Block valueBlock, int position)
        {
            TypedMapHeap typedHeap = getTypedMapHeap();

            size -= typedHeap.getEstimatedSize();
            typedHeap.add(keyBlock, valueBlock, position);
            size += typedHeap.getEstimatedSize();
        }

        @Override
        public final void merge(TypedMapHeapState other)
        {
            TypedMapHeap otherTypedHeap = ((AbstractTypedMapHeapState) other).getTypedMapHeap();
            if (otherTypedHeap == null) {
                return;
            }

            TypedMapHeap typedHeap = getTypedMapHeap();
            if (typedHeap == null) {
                setTypedMapHeap(otherTypedHeap);
                size += otherTypedHeap.getEstimatedSize();
            }
            else {
                size -= typedHeap.getEstimatedSize();
                typedHeap.addAll(otherTypedHeap);
                size += typedHeap.getEstimatedSize();
            }
        }

        @Override
        public final void serialize(BlockBuilder out)
        {
            TypedMapHeap typedHeap = getTypedMapHeap();
            if (typedHeap == null) {
                out.appendNull();
            }
            else {
                typedHeap.serialize(out);
            }
        }

        @Override
        public final void output(BlockBuilder out)
        {
            TypedMapHeap typedHeap = getTypedMapHeap();
            if (typedHeap == null) {
                out.appendNull();
            }
            else {
                typedHeap.output(out);
            }
        }

        @Override
        public final void deserialize(Block rowBlock)
        {
            checkState(getTypedMapHeap() == null, "State already initialized");

            TypedMapHeap typedHeap = deserializer.apply(rowBlock);
            setTypedMapHeap(typedHeap);
            size += typedHeap.getEstimatedSize();
        }

        @Override
        final TypedMapHeap getTypedMapHeap()
        {
            return heaps.get(groupId);
        }

        private void setTypedMapHeap(TypedMapHeap value)
        {
            heaps.set(groupId, value);
        }
    }

    public abstract static class SingleTypedMapHeapState
            extends AbstractTypedMapHeapState
    {
        private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(SingleTypedMapHeapState.class).instanceSize());

        private final LongFunction<TypedMapHeap> heapFactory;
        private final Function<Block, TypedMapHeap> deserializer;

        private TypedMapHeap typedHeap;

        public SingleTypedMapHeapState(LongFunction<TypedMapHeap> heapFactory, Function<Block, TypedMapHeap> deserializer)
        {
            this.heapFactory = heapFactory;
            this.deserializer = deserializer;
        }

        // for copying
        protected SingleTypedMapHeapState(SingleTypedMapHeapState state)
        {
            this.heapFactory = state.heapFactory;
            this.deserializer = state.deserializer;

            if (state.typedHeap != null) {
                this.typedHeap = state.typedHeap.copy();
            }
            else {
                this.typedHeap = null;
            }
        }

        @Override
        public abstract AccumulatorState copy();

        @Override
        public final long getEstimatedSize()
        {
            return INSTANCE_SIZE + (typedHeap == null ? 0 : typedHeap.getEstimatedSize());
        }

        @Override
        public final void initialize(long n)
        {
            if (typedHeap == null) {
                typedHeap = heapFactory.apply(n);
            }
        }

        @Override
        public final void add(Block keyBlock, Block valueBlock, int position)
        {
            typedHeap.add(keyBlock, valueBlock, position);
        }

        @Override
        public final void merge(TypedMapHeapState other)
        {
            TypedMapHeap otherTypedHeap = ((AbstractTypedMapHeapState) other).getTypedMapHeap();
            if (otherTypedHeap == null) {
                return;
            }
            if (typedHeap == null) {
                typedHeap = otherTypedHeap;
            }
            else {
                typedHeap.addAll(otherTypedHeap);
            }
        }

//        @Override
//        public final void popAll(BlockBuilder out)
//        {
//            if (typedHeap == null || typedHeap.isEmpty()) {
//                out.appendNull();
//                return;
//            }
//
//            BlockBuilder arrayBlockBuilder = out.beginBlockEntry();
//            typedHeap.popAllReverse(arrayBlockBuilder);
//            out.closeEntry();
//        }

        @Override
        public final void serialize(BlockBuilder out)
        {
            if (typedHeap == null) {
                out.appendNull();
            }
            else {
                typedHeap.serialize(out);
            }
        }

        @Override
        public final void output(BlockBuilder out)
        {
            if (typedHeap == null) {
                out.appendNull();
            }
            else {
                typedHeap.output(out);
            }
        }

        @Override
        public final void deserialize(Block rowBlock)
        {
            typedHeap = deserializer.apply(rowBlock);
        }

        @Override
        final TypedMapHeap getTypedMapHeap()
        {
            return typedHeap;
        }
    }
}
