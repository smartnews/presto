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
package io.prestosql.plugin.smartnews.functions.aggregation;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AggregationFunction;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.function.InputFunction;
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.VarbinaryType;
import net.agkn.hll.HLL;

import java.nio.ByteBuffer;

@AggregationFunction("pfmerge")
public final class HyperLogLogMergeAggregation
{
    private HyperLogLogMergeAggregation()
    {
    }

    private static HLL unpack(SliceState s)
    {
        return unpack(s.getSlice());
    }

    private static HLL unpack(Slice s)
    {
        if (s == null) {
            return null;
        }
        else {
            byte[] bytes = s.getBytes();
            return bytes == null ? null : HLL.fromBytes(bytes);
        }
    }

    private static Slice pack(HLL h)
    {
        return Slices.wrappedBuffer(ByteBuffer.wrap(h.toBytes()));
    }

    @InputFunction
    public static void input(SliceState state, @SqlType(StandardTypes.VARBINARY) Slice value)
    {
        HLL s = unpack(state);
        if (s == null) {
            state.setSlice(value);
        }
        else {
            s.union(unpack(value));
            state.setSlice(pack(s));
        }
    }

    @CombineFunction
    public static void combine(SliceState state, SliceState otherState)
    {
        HLL s = unpack(state);
        if (s == null) {
            state.setSlice(otherState.getSlice());
        }
        else {
            s.union(unpack(otherState));
            state.setSlice(pack(s));
        }
    }

    @OutputFunction(StandardTypes.VARBINARY)
    public static void output(SliceState state, BlockBuilder out)
    {
        VarbinaryType.VARBINARY.writeSlice(out, state.getSlice());
    }
}
