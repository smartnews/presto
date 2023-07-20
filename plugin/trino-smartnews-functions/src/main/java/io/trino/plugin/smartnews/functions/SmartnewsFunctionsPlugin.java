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
package io.trino.plugin.smartnews.functions;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.smartnews.event.QueryEventLoggerFactory;
import io.trino.plugin.smartnews.functions.actions.ChooseNotInterestedParseFunctions;
import io.trino.plugin.smartnews.functions.actions.FeedSurveyThumbClickParseFunctions;
import io.trino.plugin.smartnews.functions.actions.PostParseFunctions;
import io.trino.plugin.smartnews.functions.actions.ReportImpressionsParseFunctions;
import io.trino.plugin.smartnews.functions.actions.ReportParseFunctions;
import io.trino.plugin.smartnews.functions.actions.ViewChannelParseFunctions;
import io.trino.plugin.smartnews.functions.actions.ViewOpenArticleParseFunctions;
import io.trino.plugin.smartnews.functions.actions.ViewOriginalPageParseFunctions;
import io.trino.plugin.smartnews.functions.aggregation.HyperLogLogAggregation;
import io.trino.plugin.smartnews.functions.aggregation.HyperLogLogMergeAggregation;
import io.trino.plugin.smartnews.functions.aggregation.MapWeightedCountAggregation;
import io.trino.plugin.smartnews.functions.aggregation.map.MapUnionAggregation;
import io.trino.plugin.smartnews.functions.aggregation.videoview.ViewedSegmentsOrAggregation;
import io.trino.spi.Plugin;
import io.trino.spi.eventlistener.EventListenerFactory;
import io.trino.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class SmartnewsFunctionsPlugin
        implements Plugin
{
    private TypeManager typeManager;

    @Inject
    public void setTypeManager(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(HyperLogLogAggregation.class)
                .add(HyperLogLogMergeAggregation.class)
                .add(HyperLogLogFunctions.class)
                .add(HttpFunctions.class)
                .add(AwsFunctions.class)
                .add(AssertFunctions.class)
                .add(ExtendedFunctions.class)
                .add(ExtendedMapFunctions.class)
                .add(PseudonymisationFunctions.class)
                .add(HashFunctions.class)
                .add(MapWeightedCountAggregation.class)
                .add(ExtractHtmlTextFunction.class)
                .add(FeatureTransformFunction.class)
                .add(JsonFunctions.class)
                .add(MapFunctions.class)
                .add(MapUnionAggregation.class)
                .add(HiveHashFunction.class)
                .add(VideoFunctions.class)
                .add(ViewedSegmentsOrAggregation.class)
                .add(ReportImpressionsParseFunctions.class)
                .add(ViewChannelParseFunctions.class)
                .add(ViewOriginalPageParseFunctions.class)
                .add(ViewOpenArticleParseFunctions.class)
                .add(PostParseFunctions.class)
                .add(ReportParseFunctions.class)
                .add(ChooseNotInterestedParseFunctions.class)
                .add(FeedSurveyThumbClickParseFunctions.class)
                .build();
    }

    @Override
    public Iterable<EventListenerFactory> getEventListenerFactories()
    {
        return ImmutableSet.of(new QueryEventLoggerFactory());
    }
}
