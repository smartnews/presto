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

import io.trino.plugin.smartnews.functions.aggregation.state.TypedMapHeapState;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;

public class TypedMapHeapMinMaxStateSerializer
        implements AccumulatorStateSerializer<TypedMapHeapState>
{
    private final Type serializedType;

    public TypedMapHeapMinMaxStateSerializer(@TypeParameter("ROW(BIGINT, ARRAY(K), ARRAY(V))") Type serializedType)
    {
        this.serializedType = serializedType;
    }

    @Override
    public Type getSerializedType()
    {
        return serializedType;
    }

    @Override
    public void serialize(TypedMapHeapState state, BlockBuilder out)
    {
        state.serialize(out);
    }

    @Override
    public void deserialize(Block block, int index, TypedMapHeapState state)
    {
        Block rowBlock = (Block) serializedType.getObject(block, index);
        state.deserialize(rowBlock);
    }
}
