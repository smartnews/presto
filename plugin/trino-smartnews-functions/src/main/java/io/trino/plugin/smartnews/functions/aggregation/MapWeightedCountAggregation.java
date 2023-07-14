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
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.util.HashMap;
import java.util.Map;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;

@AggregationFunction("map_weighted_count")
public class MapWeightedCountAggregation
{
    private MapWeightedCountAggregation() {}

    public static Map<Slice, Double> newStateValue()
    {
        return new HashMap<Slice, Double>();
    }

    @InputFunction
    public static void input(@AggregationState MapString2DoubleState state, @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice key, @SqlNullable @SqlType(StandardTypes.DOUBLE) double weight)
    {
        Map<Slice, Double> m = getOrCreateState(state);
        if (key != null) {
            m.compute(key, (k, v) -> (v == null) ? weight : v + weight);
        }
    }

    @InputFunction
    public static void input(@AggregationState MapString2DoubleState state, @SqlType("map(varchar,double)") Block mapblock, @SqlType(StandardTypes.DOUBLE) double weight)
    {
        Map<Slice, Double> m = getOrCreateState(state);
        if (mapblock != null) {
            for (int keyPosition = 0; keyPosition < mapblock.getPositionCount(); keyPosition += 2) {
                Object key = readNativeValue(VARCHAR, mapblock, keyPosition);
                Object value = readNativeValue(DOUBLE, mapblock, keyPosition + 1);
                if (key instanceof Slice && value instanceof Double) {
                    m.compute(((Slice) key), (k, v) -> (v == null) ? ((Double) value) * weight : v + ((Double) value) * weight);
                }
            }
        }
    }

    @InputFunction
    public static void inputArray(@AggregationState MapString2DoubleState state, @SqlType("array(varchar)") Block mapblock, @SqlType(StandardTypes.DOUBLE) double weight)
    {
        Map<Slice, Double> m = getOrCreateState(state);
        if (mapblock != null) {
            for (int i = 0; i < mapblock.getPositionCount(); i++) {
                Object key = readNativeValue(VARCHAR, mapblock, i);
                if (key instanceof Slice keyslice) {
                    m.compute(keyslice, (k, v) -> (v == null) ? weight : v + weight);
                }
            }
        }
    }

    private static Map<Slice, Double> getOrCreateState(@AggregationState MapString2DoubleState state)
    {
        Map<Slice, Double> m = state.getMap();
        if (m == null) {
            m = newStateValue();
            state.setMap(m);
        }
        return m;
    }

    @CombineFunction
    public static void combineState(@AggregationState MapString2DoubleState state, @AggregationState MapString2DoubleState otherState)
    {
        Map<Slice, Double> input = otherState.getMap();

        Map<Slice, Double> previous = state.getMap();
        if (previous == null) {
            state.setMap(input);
        }
        else {
            for (Map.Entry<Slice, Double> entry : input.entrySet()) {
                previous.compute(entry.getKey(), (k, v) -> (v == null) ? entry.getValue() : v + entry.getValue());
            }
        }
    }

    @OutputFunction("map(varchar,double)")
    public static void evaluateFinal(@AggregationState MapString2DoubleState state, BlockBuilder out)
    {
        Map<Slice, Double> m = state.getMap();

        BlockBuilder entryBuilder = out.beginBlockEntry();
        if (m != null) {
            for (Map.Entry<Slice, Double> entry : m.entrySet()) {
                VARCHAR.writeSlice(entryBuilder, entry.getKey());
                DOUBLE.writeDouble(entryBuilder, entry.getValue());
            }
        }
        out.closeEntry();
    }
}
