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

import io.trino.plugin.smartnews.functions.aggregation.state.KeyValuePairsSumState;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Convention;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;

@AggregationFunction("map_union_sum")
public class MapUnionSumAggregation
{
    private MapUnionSumAggregation() {}

    @InputFunction
    @TypeParameter("K")
    @TypeParameter("V")
    public static void input(
            @TypeParameter("K") Type keyType,
            @OperatorDependency(
                    operator = OperatorType.EQUAL,
                    argumentTypes = {"K", "K"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = NULLABLE_RETURN))
            MethodHandle keyEqual,
            @OperatorDependency(
                    operator = OperatorType.HASH_CODE,
                    argumentTypes = "K",
                    convention = @Convention(arguments = BLOCK_POSITION, result = FAIL_ON_NULL))
            MethodHandle keyHashCode,
            @TypeParameter("V") Type valueType,
            @AggregationState({"K", "V"}) KeyValuePairsSumState state,
            @SqlType("map(K,V)") Block value)
    {
        KeyValuePairsSum pairs = state.get();
        if (pairs == null) {
            pairs = new KeyValuePairsSum(keyType, keyEqual, keyHashCode, valueType);
            state.set(pairs);
        }

        long startSize = pairs.estimatedInMemorySize();
        for (int i = 0; i < value.getPositionCount(); i += 2) {
            pairs.add(value, value, i, i + 1);
        }
        state.addMemoryUsage(pairs.estimatedInMemorySize() - startSize);
    }

    @CombineFunction
    public static void combine(
            @AggregationState({"K", "V"}) KeyValuePairsSumState state,
            @AggregationState({"K", "V"}) KeyValuePairsSumState otherState)
    {
        combineImpl(state, otherState);
    }

    @OutputFunction("map(K, V)")
    public static void output(@AggregationState({"K", "V"}) KeyValuePairsSumState state, BlockBuilder out)
    {
        outputImpl(state, out);
    }

    public static void combineImpl(
            @AggregationState({"K", "V"}) KeyValuePairsSumState state,
            @AggregationState({"K", "V"}) KeyValuePairsSumState otherState)
    {
        if (state.get() != null && otherState.get() != null) {
            Block keys = otherState.get().getKeys();
            ArrayList<Object> values = otherState.get().values;
            KeyValuePairsSum pairs = state.get();
            long startSize = pairs.estimatedInMemorySize();
            for (int i = 0; i < keys.getPositionCount(); i++) {
                pairs.add(keys, values, i, i);
            }
            state.addMemoryUsage(pairs.estimatedInMemorySize() - startSize);
        }
        else if (state.get() == null) {
            state.set(otherState.get());
        }
    }

    public static void outputImpl(@AggregationState({"K", "V"}) KeyValuePairsSumState state, BlockBuilder out)
    {
        KeyValuePairsSum pairs = state.get();
        if (pairs == null) {
            out.appendNull();
        }
        else {
            pairs.serialize(out);
        }
    }
}
