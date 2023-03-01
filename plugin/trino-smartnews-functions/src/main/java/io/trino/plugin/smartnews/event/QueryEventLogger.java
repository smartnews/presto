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
package io.trino.plugin.smartnews.event;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.eventlistener.SplitCompletedEvent;

import java.util.Optional;

public class QueryEventLogger
        implements EventListener
{
    private static final Logger log = Logger.get(QueryEventLogger.class);

    public QueryEventLogger()
    {
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
                Optional.empty(),
                Optional.empty(),
                metadata.getQueryState(),
                ImmutableList.of(),
                ImmutableList.of(),
                metadata.getUri(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        QueryStatistics statistics = queryCompletedEvent.getStatistics();
        QueryStatistics statisticsLite = new QueryStatistics(
                statistics.getCpuTime(),
                statistics.getFailedCpuTime(),
                statistics.getWallTime(),
                statistics.getQueuedTime(),
                statistics.getScheduledTime(),
                statistics.getFailedScheduledTime(),
                statistics.getResourceWaitingTime(),
                statistics.getAnalysisTime(),
                statistics.getPlanningTime(),
                statistics.getExecutionTime(),
                statistics.getInputBlockedTime(),
                statistics.getFailedInputBlockedTime(),
                statistics.getOutputBlockedTime(),
                statistics.getFailedOutputBlockedTime(),
                statistics.getPeakUserMemoryBytes(),
                statistics.getPeakTaskUserMemory(),
                statistics.getPeakTaskTotalMemory(),
                statistics.getPhysicalInputBytes(),
                statistics.getPhysicalInputRows(),
                statistics.getProcessedInputBytes(),
                statistics.getProcessedInputRows(),
                statistics.getInternalNetworkBytes(),
                statistics.getInternalNetworkRows(),
                statistics.getTotalBytes(),
                statistics.getTotalRows(),
                statistics.getOutputBytes(),
                statistics.getOutputRows(),
                statistics.getWrittenBytes(),
                statistics.getWrittenRows(),
                statistics.getCumulativeMemory(),
                statistics.getFailedCumulativeMemory(),
                statistics.getStageGcStatistics(),
                statistics.getCompletedSplits(),
                statistics.isComplete(),
                statistics.getCpuTimeDistribution(),
                statistics.getOutputBufferUtilization(),
                statistics.getOperatorSummaries(),
                statistics.getPlanNodeStatsAndCosts());

        return new QueryCompletedEvent(
                metadataLite,
                statisticsLite,
                queryCompletedEvent.getContext(),
                queryCompletedEvent.getIoMetadata(),
                queryCompletedEvent.getFailureInfo(),
                queryCompletedEvent.getWarnings(),
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
