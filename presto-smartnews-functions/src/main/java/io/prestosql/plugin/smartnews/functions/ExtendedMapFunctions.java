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
package io.prestosql.plugin.smartnews.functions;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.Type;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

@ScalarFunction
public final class ExtendedMapFunctions
{
    private final PageBuilder pageBuilder;

    private static final String MAP_VARCHAR_DOUBLE = "map(varchar,double)";

    public ExtendedMapFunctions(@TypeParameter("map(varchar,double)") Type mapType)
    {
        pageBuilder = new PageBuilder(ImmutableList.of(mapType));
    }

    @SqlType(MAP_VARCHAR_DOUBLE)
    public Block mapPercentage(@SqlType("map(varchar,bigint)") Block block)
    {
        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }

        BlockBuilder mapBlockBuilder = pageBuilder.getBlockBuilder(0);
        BlockBuilder blockBuilder = mapBlockBuilder.beginBlockEntry();

        double sum = 0;
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            sum += BIGINT.getLong(block, i + 1);
        }

        for (int i = 0; i < block.getPositionCount(); i += 2) {
            VARCHAR.appendTo(block, i, blockBuilder);
            DOUBLE.writeDouble(blockBuilder, BIGINT.getLong(block, i + 1) / sum);
        }

        mapBlockBuilder.closeEntry();
        pageBuilder.declarePosition();
        return mapBlockBuilder.getObject(mapBlockBuilder.getPositionCount() - 1, Block.class);
    }
}
