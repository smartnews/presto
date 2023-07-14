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

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.BigintType;

import java.util.HashMap;
import java.util.Map;

import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;

@AggregationFunction("union_map")
public class MapUnionAggregation
{
    private MapUnionAggregation() {}

    public static Map<Slice, Long> newStateValue()
    {
        return new HashMap<Slice, Long>();
    }

    @InputFunction
    public static void input(@AggregationState MapString2LongState state, @SqlType("map(varchar,bigint)") Block mapblock)
    {
        Map<Slice, Long> m = getOrCreateState(state);

        if (mapblock != null) {
            for (int keyPosition = 0; keyPosition < mapblock.getPositionCount(); keyPosition += 2) {
                Object keyObj = readNativeValue(VARCHAR, mapblock, keyPosition);
                Object valueObj = readNativeValue(BigintType.BIGINT, mapblock, keyPosition + 1);
                if (keyObj instanceof Slice key && valueObj instanceof Long value) {
                    m.compute(key, (k, v) -> (v == null) ? value : v + value);
                }
            }
        }
    }

    private static Map<Slice, Long> getOrCreateState(@AggregationState MapString2LongState state)
    {
        Map<Slice, Long> m = state.getMap();
        if (m == null) {
            m = newStateValue();
            state.setMap(m);
        }
        return m;
    }

    @CombineFunction
    public static void combineState(@AggregationState MapString2LongState state, @AggregationState MapString2LongState otherState)
    {
        Map<Slice, Long> input = otherState.getMap();

        Map<Slice, Long> previous = state.getMap();
        if (previous == null) {
            state.setMap(input);
        }
        else {
            for (Map.Entry<Slice, Long> entry : input.entrySet()) {
                previous.compute(entry.getKey(), (k, v) -> (v == null) ? entry.getValue() : v + entry.getValue());
            }
        }
    }

    @OutputFunction("map(varchar,bigint)")
    public static void evaluateFinal(@AggregationState MapString2LongState state, BlockBuilder out)
    {
        Map<Slice, Long> m = state.getMap();

        if (m != null) {
            BlockBuilder entryBuilder = out.beginBlockEntry();
            for (Map.Entry<Slice, Long> entry : m.entrySet()) {
                VARCHAR.writeSlice(entryBuilder, entry.getKey());
                BigintType.BIGINT.writeLong(entryBuilder, entry.getValue());
            }
            out.closeEntry();
        }
        else {
            out.appendNull();
        }
    }
}
