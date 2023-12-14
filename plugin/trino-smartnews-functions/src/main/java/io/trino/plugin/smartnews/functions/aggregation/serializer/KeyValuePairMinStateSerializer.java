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
package io.trino.plugin.smartnews.functions.aggregation.serializer;

import io.trino.plugin.smartnews.functions.aggregation.map.KeyValuePairsMin;
import io.trino.plugin.smartnews.functions.aggregation.state.KeyValuePairsMinState;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.function.Convention;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static java.util.Objects.requireNonNull;

public class KeyValuePairMinStateSerializer
        implements AccumulatorStateSerializer<KeyValuePairsMinState>
{
    private final Type mapType;
    private final MethodHandle keyEqual;
    private final MethodHandle keyHashCode;

    public KeyValuePairMinStateSerializer(
            @TypeParameter("MAP(K, V)") Type mapType,
            @OperatorDependency(
                    operator = OperatorType.EQUAL,
                    argumentTypes = {"K", "K"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = NULLABLE_RETURN))
            MethodHandle keyEqual,
            @OperatorDependency(
                    operator = OperatorType.HASH_CODE,
                    argumentTypes = "K",
                    convention = @Convention(arguments = BLOCK_POSITION, result = FAIL_ON_NULL))
            MethodHandle keyHashCode)
    {
        this.mapType = requireNonNull(mapType, "mapType is null");
        this.keyEqual = requireNonNull(keyEqual, "keyEqual is null");
        this.keyHashCode = requireNonNull(keyHashCode, "keyHashCode is null");
    }

    @Override
    public Type getSerializedType()
    {
        return mapType;
    }

    @Override
    public void serialize(KeyValuePairsMinState state, BlockBuilder out)
    {
        if (state.get() == null) {
            out.appendNull();
        }
        else {
            state.get().serialize(out);
        }
    }

    @Override
    public void deserialize(Block block, int index, KeyValuePairsMinState state)
    {
        state.set(new KeyValuePairsMin((Block) mapType.getObject(block, index), state.getKeyType(), keyEqual, keyHashCode, state.getValueType()));
    }
}
