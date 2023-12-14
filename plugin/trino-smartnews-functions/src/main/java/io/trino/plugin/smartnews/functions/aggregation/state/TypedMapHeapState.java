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
package io.trino.plugin.smartnews.functions.aggregation.state;

import io.trino.plugin.smartnews.functions.aggregation.serializer.TypedMapHeapMinMaxStateSerializer;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;

@AccumulatorStateMetadata(
        stateFactoryClass = TypedMapMaxHeapStateFactory.class,
        stateSerializerClass = TypedMapHeapMinMaxStateSerializer.class,
        typeParameters = {"K", "V"},
        serializedType = "ROW(BIGINT, ARRAY(K), ARRAY(V))")
public interface TypedMapHeapState
        extends AccumulatorState
{
    /**
     * Initialize the state if not already initialized.  Only the first call is processed and
     * all subsequent calls are ignored.
     */
    void initialize(long n);

    /**
     * Adds the value to this state.
     */
    void add(Block keyBlock, Block valueBlock, int position);

    /**
     * Merge with the specified state.
     * The supplied state should not be used after this method is called, because
     * the internal details of the state may be reused in this state.
     */
    void merge(TypedMapHeapState other);

    /**
     * Write this state to the specified block builder.
     */
    void serialize(BlockBuilder out);

    /**
     * Read the state to the specified block builder.
     *
     * @throws IllegalStateException if state is already initialized
     */
    void deserialize(Block rowBlock);

    /**
     * Output this state to the specified block builder.
     */
    void output(BlockBuilder out);
}
