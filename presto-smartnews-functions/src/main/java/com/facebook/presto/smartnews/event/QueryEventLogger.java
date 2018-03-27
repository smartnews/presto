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
package com.facebook.presto.smartnews.event;

import com.alibaba.fastjson.JSON;
import com.amazonaws.services.s3.model.BucketLoggingConfiguration;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.QueryStatistics;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import io.airlift.log.Logger;

import java.util.Collections;
import java.util.Optional;

public class QueryEventLogger
        implements EventListener
{
    private static final Logger log = Logger.get(QueryEventLogger.class);

    public QueryEventLogger() {
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
    }

    private static QueryCompletedEvent asLiteEvent(QueryCompletedEvent queryCompletedEvent)
    {
        QueryMetadata metadata = queryCompletedEvent.getMetadata();
        QueryMetadata metadataLite = new QueryMetadata(
                metadata.getQueryId(),
                metadata.getTransactionId(),
                metadata.getQuery(),
                metadata.getQueryState(),
                metadata.getUri(),
                Optional.empty(),
                Optional.empty());

        QueryStatistics statistics = queryCompletedEvent.getStatistics();
        QueryStatistics statisticsLite = new QueryStatistics(
                statistics.getCpuTime(),
                statistics.getWallTime(),
                statistics.getQueuedTime(),
                statistics.getAnalysisTime(),
                statistics.getDistributedPlanningTime(),
                statistics.getPeakUserMemoryBytes(),
                statistics.getPeakTotalNonRevocableMemoryBytes(),
                statistics.getTotalBytes(),
                statistics.getTotalRows(),
                statistics.getOutputBytes(),
                statistics.getOutputRows(),
                statistics.getWrittenBytes(),
                statistics.getWrittenRows(),
                statistics.getCumulativeMemory(),
                statistics.getStageGcStatistics(),
                statistics.getCompletedSplits(),
                statistics.isComplete(),
                Collections.EMPTY_LIST,
                Collections.EMPTY_LIST);

        return new QueryCompletedEvent(
                metadataLite,
                statisticsLite,
                queryCompletedEvent.getContext(),
                queryCompletedEvent.getIoMetadata(),
                queryCompletedEvent.getFailureInfo(),
                queryCompletedEvent.getCreateTime(),
                queryCompletedEvent.getExecutionStartTime(),
                queryCompletedEvent.getEndTime());
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        log.info("%s", JSON.toJSONString(asLiteEvent(queryCompletedEvent)));
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
    }
}
