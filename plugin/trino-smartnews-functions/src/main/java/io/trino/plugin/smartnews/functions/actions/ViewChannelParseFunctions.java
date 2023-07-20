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
import com.smartnews.dmp.core.data.actions.ViewChannelAction;
import com.smartnews.dmp.core.data.actions.ViewChannelData;
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
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.spi.type.VarcharType.VARCHAR;

public class ViewChannelParseFunctions
{
    public static List<Utils.TypeAndName> actionSpecificTypes = Arrays.asList(
            Utils.TypeAndName.of("channel", VarcharType.VARCHAR),
            Utils.TypeAndName.of("index_duration", DoubleType.DOUBLE),
            Utils.TypeAndName.of("total_duration", DoubleType.DOUBLE),
            Utils.TypeAndName.of("links", new ArrayType(
                    RowType.from(ImmutableList.of(
                            RowType.field("link_id", BigintType.BIGINT),
                            RowType.field("index", IntegerType.INTEGER),
                            RowType.field("block", VarcharType.VARCHAR),
                            RowType.field("req_id", VarcharType.VARCHAR),
                            RowType.field("tracking_token", new MapType(VARCHAR, VARCHAR, new TypeOperators())),
                            RowType.field("cell_type", IntegerType.INTEGER))))));

    // concat the base types with action specific types
    private static final RowType ROW_TYPE = RowType.anonymous(
            Stream.concat(Utils.baseTypes.stream(), actionSpecificTypes.stream())
                    .map(r -> r.type)
                    .collect(Collectors.toList()));

    private final PageBuilder pageBuilder;
    protected transient SmartNewsActionParser parser = new SmartNewsActionParser();

    public ViewChannelParseFunctions()
    {
        pageBuilder = new PageBuilder(ImmutableList.of(ROW_TYPE));
    }

    @Description("parse action viewChannel")
    @ScalarFunction("parse_view_channel")
    @SqlType("row(device_token varchar, virtual_id varchar, edition varchar, platform varchar, session_id varchar, app_version varchar, system_version varchar, connection_type varchar, locale varchar, timezone varchar, hardware varchar, event_timestamp bigint, account_id varchar, user_creation_timestamp bigint, ab_test array(map(varchar, integer)), ab_test_v3_variant_ids array(integer), " +
            "channel varchar, index_duration double, total_duration double, links array(row(link_id bigint, index integer, block varchar, req_id varchar, tracking_token map(varchar, varchar), cell_type integer)))")
    @SqlNullable
    public Block parse(@SqlType(StandardTypes.VARCHAR) Slice json)
            throws Exception
    {
        ViewChannelAction record = (ViewChannelAction) parser.parseEssential(json.toStringUtf8());

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

    public void writeActionSpecific(BlockBuilder builder, ViewChannelAction record)
    {
        Utils.writeString(builder, record.getData().getChannel());
        Utils.writeDouble(builder, record.getData().getIndexDuration());
        Utils.writeDouble(builder, record.getData().getTotalDuration());

        // links
        List<Object[]> links = getLinks(record);
        if (links == null) {
            builder.appendNull();
        }
        else {
            BlockBuilder linksBuilder = builder.beginBlockEntry();

            for (Object[] objs : links) {
                BlockBuilder rowBuilder = linksBuilder.beginBlockEntry();
                Utils.writeLong(rowBuilder, objs[0]); // link_id
                Utils.writeInt(rowBuilder, objs[1]);  // index
                Utils.writeString(rowBuilder, objs[2]);    // block
                Utils.writeString(rowBuilder, objs[3]);    // req_id

                // tracking_token
                if (objs[4] == null || true) {
                    rowBuilder.appendNull();
                }
                else {
                    Map<String, String> trackingToken = (Map<String, String>) objs[4];
                    BlockBuilder mapBuilder = rowBuilder.beginBlockEntry();
                    for (Map.Entry<String, String> entry : trackingToken.entrySet()) {
                        Utils.writeString(mapBuilder, entry.getKey());
                        Utils.writeString(mapBuilder, entry.getValue());
                    }
                    rowBuilder.closeEntry();
                }

                Utils.writeInt(rowBuilder, objs[5]);    // cell_type
                linksBuilder.closeEntry();
            }
            builder.closeEntry();
        }
    }

    public static List<Object[]> getLinks(ViewChannelAction action)
    {
        ViewChannelData data = action.getData();
        Long[] linkIds = data.getLinkIds();
        Integer[] linkBlockIndices = data.getLinkBlockIndices();
        String[] trackingTokens = data.getTrackingTokens();
        String[] blockIds = data.getBlockIds();
        List<Object[]> links = new ArrayList<>();
        if (linkIds != null) {
            for (int i = 0; i < linkIds.length; i++) {
                Object[] o = new Object[6];
                o[0] = linkIds[i];
                o[1] = Integer.valueOf(i);
                if (linkBlockIndices != null && blockIds != null &&
                        i < linkBlockIndices.length &&
                        linkBlockIndices[i] < blockIds.length && linkBlockIndices[i] >= 0) {
                    o[2] = blockIds[linkBlockIndices[i]];
                }
                else {
                    o[2] = null;
                }

                if (trackingTokens != null && i < trackingTokens.length) {
                    Map<String, String> trackingToken = action.parseTrackingToken(trackingTokens[i]);
                    o[3] = trackingToken != null ? trackingToken.get("1.1") : null;
                    o[4] = trackingToken;
                }
                else {
                    o[3] = null;
                    o[4] = null;
                }
                o[5] = null;

                links.add(o);
            }
        }
        return links;
    }
}
