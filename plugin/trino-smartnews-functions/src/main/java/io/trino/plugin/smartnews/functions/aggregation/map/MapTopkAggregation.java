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

import io.trino.plugin.smartnews.functions.aggregation.state.KeyValuePairsTopkState;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Convention;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;

@AggregationFunction("map_agg_topk")
public class MapTopkAggregation
{
    private MapTopkAggregation() {}

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
                    operator = OperatorType.COMPARISON_UNORDERED_LAST,
                    argumentTypes = {"V", "V"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = NULLABLE_RETURN))
            MethodHandle valueCompare,
            @TypeParameter("V") Type valueType,
            @AggregationState({"K", "V"}) KeyValuePairsTopkState state,
            @SqlType(StandardTypes.INTEGER) long k,
            @BlockPosition @SqlType("K") Block keyBlock,
            @BlockPosition @SqlType("V") Block valueBlock,
            @BlockIndex int position)
    {
        KeyValuePairsTopk pairs = state.get();
        if (pairs == null) {
            pairs = new KeyValuePairsTopk(keyType, valueType, keyEqual, valueCompare, (int) k);
            state.set(pairs);
        }

        long startSize = pairs.estimatedInMemorySize();
        pairs.add(keyBlock, position, valueBlock, position);
        state.addMemoryUsage(pairs.estimatedInMemorySize() - startSize);
    }

    @CombineFunction
    public static void combine(
            @AggregationState({"K", "V"}) KeyValuePairsTopkState state,
            @AggregationState({"K", "V"}) KeyValuePairsTopkState otherState)
    {
        combineImpl(state, otherState);
    }

    @OutputFunction("map(K, V)")
    public static void output(@AggregationState({"K", "V"}) KeyValuePairsTopkState state, BlockBuilder out)
    {
        outputImpl(state, out);
    }

    public static void combineImpl(
            @AggregationState({"K", "V"}) KeyValuePairsTopkState state,
            @AggregationState({"K", "V"}) KeyValuePairsTopkState otherState)
    {
        if (state.get() != null && otherState.get() != null) {
            ArrayList<Block> keys = otherState.get().getKeys();
            ArrayList<Block> values = otherState.get().getValues();
            KeyValuePairsTopk pairs = state.get();
            long startSize = pairs.estimatedInMemorySize();
            for (int i = 0; i < keys.size(); i++) {
                pairs.add(keys, i, values, i);
            }
            state.addMemoryUsage(pairs.estimatedInMemorySize() - startSize);
        }
        else if (state.get() == null) {
            state.set(otherState.get());
        }
    }

    public static void outputImpl(@AggregationState({"K", "V"}) KeyValuePairsTopkState state, BlockBuilder out)
    {
        KeyValuePairsTopk pairs = state.get();
        if (pairs == null) {
            out.appendNull();
        }
        else {
            pairs.serialize(out);
        }
    }
}
