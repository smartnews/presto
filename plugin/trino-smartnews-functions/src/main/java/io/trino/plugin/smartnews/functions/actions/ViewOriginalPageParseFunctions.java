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
import com.smartnews.dmp.core.data.actions.ViewOriginalPageAction;
import com.smartnews.dmp.core.data.actions.ViewOriginalPageData;
import io.airlift.slice.Slice;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.spi.type.VarcharType.VARCHAR;

public class ViewOriginalPageParseFunctions
{
    public static List<Utils.TypeAndName> actionSpecificTypes = Arrays.asList(
            Utils.TypeAndName.of("link_id", BigintType.BIGINT),
            Utils.TypeAndName.of("url", VarcharType.VARCHAR),
            Utils.TypeAndName.of("channel", VarcharType.VARCHAR),
            Utils.TypeAndName.of("placement", VarcharType.VARCHAR),
            Utils.TypeAndName.of("read_time_web", DoubleType.DOUBLE),
            Utils.TypeAndName.of("read_time_smart", DoubleType.DOUBLE),
            Utils.TypeAndName.of("view_ratio_web", DoubleType.DOUBLE),
            Utils.TypeAndName.of("view_ratio_smart", DoubleType.DOUBLE),
            Utils.TypeAndName.of("block", VarcharType.VARCHAR),
            Utils.TypeAndName.of("req_id", VarcharType.VARCHAR),
            Utils.TypeAndName.of("tracking_token", new MapType(VARCHAR, VARCHAR, new TypeOperators())));

    // concat the base types with action specific types
    private static final RowType ROW_TYPE = RowType.anonymous(
            Stream.concat(Utils.baseTypes.stream(), actionSpecificTypes.stream())
                    .map(r -> r.type)
                    .collect(Collectors.toList()));

    private final PageBuilder pageBuilder;
    protected transient SmartNewsActionParser parser = new SmartNewsActionParser();

    public ViewOriginalPageParseFunctions()
    {
        pageBuilder = new PageBuilder(ImmutableList.of(ROW_TYPE));
    }

    @Description("parse action viewOriginalPage")
    @ScalarFunction("parse_view_original_page")
    @SqlType("row(device_token varchar, virtual_id varchar, edition varchar, platform varchar, session_id varchar, app_version varchar, system_version varchar, connection_type varchar, locale varchar, timezone varchar, hardware varchar, event_timestamp bigint, account_id varchar, user_creation_timestamp bigint, ab_test array(map(varchar, integer)), ab_test_v3_variant_ids array(integer), " +
            "link_id bigint, url varchar, channel varchar, placement varchar, read_time_web double, read_time_smart double, view_ratio_web double, view_ratio_smart double, block varchar, req_id varchar, tracking_token map(varchar, varchar))")
    @SqlNullable
    public Block parse(@SqlType(StandardTypes.VARCHAR) Slice json)
            throws Exception
    {
        ViewOriginalPageAction record = (ViewOriginalPageAction) parser.parseEssential(json.toStringUtf8());

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

    public void writeActionSpecific(BlockBuilder builder, ViewOriginalPageAction record)
    {
        ViewOriginalPageData data = record.getData();
        Utils.writeLong(builder, data.getLinkId());
        Utils.writeString(builder, data.getUrl());
        Utils.writeString(builder, data.getChannel());
        Utils.writeString(builder, data.getPlacement());
        Utils.writeDouble(builder, data.getReadTimeWeb());
        Utils.writeDouble(builder, data.getReadTimeSmart());
        Utils.writeDouble(builder, data.getViewRatioWeb());
        Utils.writeDouble(builder, data.getViewRatioSmart());
        Utils.writeString(builder, data.getBlock());
        Map<String, String> trackingToken = record.parseTrackingToken(data.getTrackingToken());
        String reqId = trackingToken != null ? trackingToken.get("1.1") : null;
        Utils.writeString(builder, reqId);

        BlockBuilder mapBuilder = builder.beginBlockEntry();
        for (Map.Entry<String, String> entry : trackingToken.entrySet()) {
            Utils.writeString(mapBuilder, entry.getKey());
            Utils.writeString(mapBuilder, entry.getValue());
        }
        builder.closeEntry();
    }
}
