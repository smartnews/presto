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
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;

import java.util.HashMap;
import java.util.Map;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class MapWeightedCountStateSerializer
        implements AccumulatorStateSerializer<MapString2DoubleState>
{
    private final MapType mapType;

    public MapWeightedCountStateSerializer()
    {
        mapType = new MapType(VARCHAR, DOUBLE, new TypeOperators());
    }

    @Override
    public Type getSerializedType()
    {
        return mapType;
    }

    @Override
    public void serialize(MapString2DoubleState state, BlockBuilder out)
    {
        if (state.getMap() == null) {
            out.appendNull();
        }
        else {
            Map<Slice, Double> m = state.getMap();
            BlockBuilder entryBuilder = out.beginBlockEntry();
            for (Map.Entry<Slice, Double> entry : m.entrySet()) {
                VARCHAR.writeSlice(entryBuilder, entry.getKey());
                DOUBLE.writeDouble(entryBuilder, entry.getValue());
            }
            out.closeEntry();
        }
    }

    @Override
    public void deserialize(Block block, int index, MapString2DoubleState state)
    {
        state.setMap(null);

        Map<Slice, Double> map = new HashMap<>();
        Block singleMapBlock = mapType.getObject(block, index);
        for (int keyPosition = 0; keyPosition < singleMapBlock.getPositionCount(); keyPosition += 2) {
            Object key = readNativeValue(VARCHAR, singleMapBlock, keyPosition);
            Object value = readNativeValue(DOUBLE, singleMapBlock, keyPosition + 1);
            if (key instanceof Slice && value instanceof Double) {
                map.put(((Slice) key), (Double) value);
            }
        }
        state.setMap(map);
    }
}
