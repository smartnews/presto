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
package io.trino.plugin.smartnews.functions.actions;

import com.google.common.collect.ImmutableList;
import com.smartnews.dmp.core.SmartNewsActionParser;
import com.smartnews.dmp.core.data.actions.ChooseNotInterestedAction;
import com.smartnews.dmp.core.data.actions.ChooseNotInterestedData;
import io.airlift.slice.Slice;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.VarcharType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ChooseNotInterestedParseFunctions
{
    public static List<Utils.TypeAndName> actionSpecificTypes = Arrays.asList(
            Utils.TypeAndName.of("link_id", BigintType.BIGINT),
            Utils.TypeAndName.of("publisher_name", VarcharType.VARCHAR),
            Utils.TypeAndName.of("url", VarcharType.VARCHAR),
            Utils.TypeAndName.of("link_block_indices", new ArrayType(IntegerType.INTEGER)),
            Utils.TypeAndName.of("city", VarcharType.VARCHAR),
            Utils.TypeAndName.of("region", VarcharType.VARCHAR),
            Utils.TypeAndName.of("country_iso_code", VarcharType.VARCHAR),
            Utils.TypeAndName.of("lat", DoubleType.DOUBLE),
            Utils.TypeAndName.of("lon", DoubleType.DOUBLE));

    // concat the base types with action specific types
    private static final RowType ROW_TYPE = RowType.anonymous(
            Stream.concat(Utils.baseTypes.stream(), actionSpecificTypes.stream())
                    .map(r -> r.type)
                    .collect(Collectors.toList()));

    private final PageBuilder pageBuilder;
    protected transient SmartNewsActionParser parser = new SmartNewsActionParser();

    public ChooseNotInterestedParseFunctions()
    {
        pageBuilder = new PageBuilder(ImmutableList.of(ROW_TYPE));
    }

    @Description("parse action chooseNotInterested")
    @ScalarFunction("parse_choose_not_interested")
    @SqlType("row(device_token varchar, virtual_id varchar, edition varchar, platform varchar, session_id varchar, app_version varchar, system_version varchar, connection_type varchar, locale varchar, timezone varchar, hardware varchar, event_timestamp bigint, account_id varchar, user_creation_timestamp bigint, ab_test array(map(varchar, integer)), ab_test_v3_variant_ids array(integer), " +
            "link_id bigint, publisher_name varchar, url varchar, link_block_indices array(integer), city varchar, region varchar, country_iso_code varchar, lat double, lon double)")
    @SqlNullable
    public Block parse(@SqlType(StandardTypes.VARCHAR) Slice json)
            throws Exception
    {
        return parseImpl(json, false);
    }

    @Description("parse action chooseNotInterested")
    @ScalarFunction("parse_choose_not_interested_null_on_bad_input")
    @SqlType("row(device_token varchar, virtual_id varchar, edition varchar, platform varchar, session_id varchar, app_version varchar, system_version varchar, connection_type varchar, locale varchar, timezone varchar, hardware varchar, event_timestamp bigint, account_id varchar, user_creation_timestamp bigint, ab_test array(map(varchar, integer)), ab_test_v3_variant_ids array(integer), " +
            "link_id bigint, publisher_name varchar, url varchar, link_block_indices array(integer), city varchar, region varchar, country_iso_code varchar, lat double, lon double)")
    @SqlNullable
    public Block parseNullOnBadInput(@SqlType(StandardTypes.VARCHAR) Slice json)
            throws Exception
    {
        return parseImpl(json, true);
    }

    public Block parseImpl(Slice json, boolean skipBadInput)
            throws Exception
    {
        ChooseNotInterestedAction record = (ChooseNotInterestedAction) parser.parseEssential(json.toStringUtf8());
        if (record == null) {
            if (skipBadInput) {
                return null;
            }
            throw new RuntimeException("Failed to parse essential: " + json.toStringUtf8());
        }
        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
        BlockBuilder rowBuilder = blockBuilder.beginBlockEntry();
        Utils.writeActionBase(rowBuilder, record);
        writeActionSpecific(rowBuilder, record);
        blockBuilder.closeEntry();
        pageBuilder.declarePositions(1);
        return ROW_TYPE.getObject(blockBuilder, blockBuilder.getPositionCount() - 1);
    }

    public void writeActionSpecific(BlockBuilder builder, ChooseNotInterestedAction record)
    {
        ChooseNotInterestedData data = record.getData();
        Utils.writeLong(builder, data.getLinkId());
        Utils.writeString(builder, data.getPublisherName());
        Utils.writeString(builder, data.getUrl());

        // link_block_indices
        if (data.getLinkBlockIndices() == null) {
            builder.appendNull();
        }
        else {
            BlockBuilder arrayBuilder = builder.beginBlockEntry();
            for (Integer linkBlockIndex : data.getLinkBlockIndices()) {
                Utils.writeInt(arrayBuilder, linkBlockIndex);
            }
            builder.closeEntry();
        }
        Utils.writeString(builder, data.getCity());
        Utils.writeString(builder, data.getRegion());
        Utils.writeString(builder, data.getCountryIsoCode());
        Utils.writeDouble(builder, data.getLatitude());
        Utils.writeDouble(builder, data.getLongitude());
    }
}
