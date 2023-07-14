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
import io.airlift.slice.Slices;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.nio.charset.StandardCharsets;

@AggregationFunction("pfaggr")
public class HyperLogLogAggregation
{
    private HyperLogLogAggregation() {}

    public static com.smartnews.dmp.core.data.HyperLogLog newHyperLogLog()
    {
        return new com.smartnews.dmp.core.data.HyperLogLog();
    }

    @InputFunction
    public static void input(@AggregationState SnHyperLogLogState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        com.smartnews.dmp.core.data.HyperLogLog hll = getOrCreateHyperLogLog(state);
        //state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(value);
        //state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    @LiteralParameters("x")
    public static void input(@AggregationState SnHyperLogLogState state, @SqlType("varchar(x)") Slice value)
    {
        com.smartnews.dmp.core.data.HyperLogLog hll = getOrCreateHyperLogLog(state);
        //state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(new String(value.getBytes(), StandardCharsets.UTF_8));
        //state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    public static void input(@AggregationState SnHyperLogLogState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        com.smartnews.dmp.core.data.HyperLogLog hll = getOrCreateHyperLogLog(state);
        //state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(value);
        //state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    private static com.smartnews.dmp.core.data.HyperLogLog getOrCreateHyperLogLog(@AggregationState SnHyperLogLogState state)
    {
        com.smartnews.dmp.core.data.HyperLogLog hll = state.getHyperLogLog();
        if (hll == null) {
            hll = newHyperLogLog();
            state.setHyperLogLog(hll);
            //state.addMemoryUsage(hll.estimatedInMemorySize());
        }
        return hll;
    }

    @CombineFunction
    public static void combineState(@AggregationState SnHyperLogLogState state, @AggregationState SnHyperLogLogState otherState)
    {
        com.smartnews.dmp.core.data.HyperLogLog input = otherState.getHyperLogLog();

        com.smartnews.dmp.core.data.HyperLogLog previous = state.getHyperLogLog();
        if (previous == null) {
            state.setHyperLogLog(input);
            //state.addMemoryUsage(input.estimatedInMemorySize());
        }
        else {
            //state.addMemoryUsage(-previous.estimatedInMemorySize());
            previous.union(input);
            //state.addMemoryUsage(previous.estimatedInMemorySize());
        }
    }

    @OutputFunction(StandardTypes.HYPER_LOG_LOG)
    public static void evaluateFinal(@AggregationState SnHyperLogLogState state, BlockBuilder out)
    {
        if (state.getHyperLogLog() == null) {
            out.appendNull();
        }
        else {
            Slice slice = Slices.wrappedBuffer(state.getHyperLogLog().toBytes());
            out.writeBytes(slice, 0, slice.length()).closeEntry();
        }
    }
}
