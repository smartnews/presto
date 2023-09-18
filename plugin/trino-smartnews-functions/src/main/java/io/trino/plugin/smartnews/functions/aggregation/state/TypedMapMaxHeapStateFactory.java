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

import io.trino.plugin.smartnews.functions.aggregation.map.TypedMapHeap;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.Convention;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.util.function.Function;
import java.util.function.LongFunction;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

public class TypedMapMaxHeapStateFactory
        implements AccumulatorStateFactory<TypedMapHeapState>
{
    private static final long MAX_NUMBER_OF_VALUES = 10_000;
    private final LongFunction<TypedMapHeap> heapFactory;
    private final Function<Block, TypedMapHeap> deserializer;

    public TypedMapMaxHeapStateFactory(
            @OperatorDependency(
                    operator = OperatorType.EQUAL,
                    argumentTypes = {"K", "K"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = NULLABLE_RETURN))
            MethodHandle keyEqual,
            @OperatorDependency(
                    operator = OperatorType.COMPARISON_UNORDERED_FIRST,
                    argumentTypes = {"V", "V"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = FAIL_ON_NULL))
            MethodHandle valueCompare,
            @TypeParameter("K") Type keyType,
            @TypeParameter("V") Type valueType)
    {
        heapFactory = n -> {
            checkCondition(n > 0, INVALID_FUNCTION_ARGUMENT, "third argument of map_agg_topk must be a positive integer");
            checkCondition(
                    n <= MAX_NUMBER_OF_VALUES,
                    INVALID_FUNCTION_ARGUMENT,
                    "third argument of map_agg_topk must be less than or equal to %s; found %s",
                    MAX_NUMBER_OF_VALUES,
                    n);
            return new TypedMapHeap(false, keyEqual, valueCompare, keyType, valueType, toIntExact(n));
        };
        // pre create the array types to avoid creating them for each state
        ArrayType keyArrayType = new ArrayType(keyType);
        ArrayType valueArrayType = new ArrayType(valueType);
        deserializer = rowBlock -> TypedMapHeap.deserialize(false, keyEqual, valueCompare, keyType, keyArrayType, valueType, valueArrayType, rowBlock);
    }

    @Override
    public TypedMapHeapState createSingleState()
    {
        return new SingleTypedMapMaxHeapState(heapFactory, deserializer);
    }

    @Override
    public TypedMapHeapState createGroupedState()
    {
        return new TypedMapHeapStateFactory.GroupedTypedMapHeapState(heapFactory, deserializer);
    }

    private static class GroupedTypedMapMaxHeapState
            extends TypedMapHeapStateFactory.GroupedTypedMapHeapState
            implements TypedMapHeapState
    {
        public GroupedTypedMapMaxHeapState(LongFunction<TypedMapHeap> heapFactory, Function<Block, TypedMapHeap> deserializer)
        {
            super(heapFactory, deserializer);
        }
    }

    private static class SingleTypedMapMaxHeapState
            extends TypedMapHeapStateFactory.SingleTypedMapHeapState
            implements TypedMapHeapState
    {
        public SingleTypedMapMaxHeapState(LongFunction<TypedMapHeap> heapFactory, Function<Block, TypedMapHeap> deserializer)
        {
            super(heapFactory, deserializer);
        }

        public SingleTypedMapMaxHeapState(SingleTypedMapMaxHeapState state)
        {
            super(state);
        }

        @Override
        public AccumulatorState copy()
        {
            return new SingleTypedMapMaxHeapState(this);
        }
    }

    public static void checkCondition(boolean condition, ErrorCodeSupplier errorCode, String formatString, Object... args)
    {
        if (!condition) {
            throw new TrinoException(errorCode, format(formatString, args));
        }
    }
}
