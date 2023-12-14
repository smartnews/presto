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

import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;

import static io.trino.spi.type.HyperLogLogType.HYPER_LOG_LOG;

public class SnHyperLogLogStateSerializer
        implements AccumulatorStateSerializer<SnHyperLogLogState>
{
    @Override
    public Type getSerializedType()
    {
        return HYPER_LOG_LOG;
    }

    @Override
    public void serialize(SnHyperLogLogState state, BlockBuilder out)
    {
        if (state.getHyperLogLog() == null) {
            out.appendNull();
        }
        else {
            HYPER_LOG_LOG.writeSlice(out, Slices.wrappedBuffer(state.getHyperLogLog().toBytes()));
        }
    }

    @Override
    public void deserialize(Block block, int index, SnHyperLogLogState state)
    {
        state.setHyperLogLog(com.smartnews.dmp.core.data.HyperLogLog.fromBytes(HYPER_LOG_LOG.getSlice(block, index).getBytes()));
    }
}
