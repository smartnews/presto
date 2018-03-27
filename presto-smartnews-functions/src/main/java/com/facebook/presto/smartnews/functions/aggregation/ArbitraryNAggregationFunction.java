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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.Math.toIntExact;

@AggregationFunction("arbitrary_n")
public class ArbitraryNAggregationFunction
{
    private ArbitraryNAggregationFunction()
    {
        //no instance
    }

    @InputFunction
    public static void input(@AggregationState ArbitraryNState state,
            @SqlNullable @SqlType("varchar") Slice slice,
            @SqlType(StandardTypes.INTEGER) long n)
    {
        if (state.getData() == null) {
            if (n <= 0) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "second argument of arbitrary_n must be positive");
            }
            ArbitraryN data = new ArbitraryN(toIntExact(n));
            state.setData(data);
        }

        if (state.getData().isFull()) {
            return;
        }

        state.getData().add(slice);
    }

    @CombineFunction
    public static void combine(@AggregationState ArbitraryNState state,
            @AggregationState ArbitraryNState otherState)
    {
        if (state.getData() == null) {
            if (otherState.getData() != null) {
                state.setData(otherState.getData());
                return;
            }
        }
        else {
            if (state.getData().isFull()) {
                return;
            }

            ArbitraryN otherData = otherState.getData();

            if (!otherData.isEmpty()) {
                int size = otherData.getPositionCount();
                for (int i = 0; i < size; i++) {
                    state.getData().add(otherState.getData().getBlockBuilder(), i);
                }
            }
        }
    }

    @OutputFunction("array<varchar>")
    public static void output(@AggregationState ArbitraryNState state,
            BlockBuilder out)
    {
        if (state.getData().isEmpty()) {
            out.appendNull();
        }
        else {
            state.getData().writeAsArray(out);
        }
    }
}
