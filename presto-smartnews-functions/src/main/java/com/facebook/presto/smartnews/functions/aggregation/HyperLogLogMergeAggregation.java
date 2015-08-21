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

import com.facebook.presto.operator.aggregation.AggregationCompiler;
import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.operator.aggregation.CombineFunction;
import com.facebook.presto.operator.aggregation.InputFunction;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.operator.aggregation.OutputFunction;
import com.facebook.presto.operator.aggregation.state.SliceState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import net.agkn.hll.HLL;

import java.nio.ByteBuffer;

@AggregationFunction("pfmerge")
public final class HyperLogLogMergeAggregation
{
    public static final InternalAggregationFunction PFMERGE =
            new AggregationCompiler().generateAggregationFunction(HyperLogLogMergeAggregation.class);

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
