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
package io.prestosql.plugin.kinesis;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.prestosql.plugin.kinesis.decoder.KinesisDecoderRegistry;
import io.prestosql.plugin.kinesis.decoder.KinesisFieldDecoder;
import io.prestosql.plugin.kinesis.decoder.KinesisRowDecoder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.RecordSet;

import java.util.List;

import static io.prestosql.plugin.kinesis.KinesisSessionProperties.getCheckpointIterationNumber;
import static io.prestosql.plugin.kinesis.KinesisSessionProperties.getCheckpointLogicalName;
import static io.prestosql.plugin.kinesis.KinesisSessionProperties.getIteratorType;
import static io.prestosql.plugin.kinesis.KinesisSessionProperties.isCheckpointEnabled;
import static java.util.Objects.requireNonNull;

public class KinesisRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final KinesisHandleResolver handleResolver;
    private final KinesisClientManager clientManager;
    private final KinesisDecoderRegistry registry;
    private final KinesisConnectorConfig kinesisConnectorConfig;

    @Inject
    public KinesisRecordSetProvider(KinesisDecoderRegistry registry,
            KinesisHandleResolver handleResolver,
            KinesisClientManager clientManager,
            KinesisConnectorConfig kinesisConnectorConfig)
    {
        this.registry = requireNonNull(registry, "registry is null");
        this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");
        this.clientManager = requireNonNull(clientManager, "clientManager is null");
        this.kinesisConnectorConfig = requireNonNull(kinesisConnectorConfig, "kinesisConnectorConfig is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        KinesisSplit kinesisSplit = handleResolver.convertSplit(split);

        ImmutableList.Builder<KinesisColumnHandle> handleBuilder = ImmutableList.builder();
        ImmutableMap.Builder<KinesisColumnHandle, KinesisFieldDecoder<?>> messageFieldDecoderBuilder = ImmutableMap.builder();

        KinesisRowDecoder messageDecoder = registry.getRowDecoder(kinesisSplit.getMessageDataFormat());

        for (ColumnHandle handle : columns) {
            KinesisColumnHandle columnHandle = handleResolver.convertColumnHandle(handle);
            handleBuilder.add(columnHandle);

            if (!columnHandle.isInternal()) {
                KinesisFieldDecoder<?> fieldDecoder = registry.getFieldDecoder(kinesisSplit.getMessageDataFormat(),
                        columnHandle.getType().getJavaType(),
                        columnHandle.getDataFormat());

                messageFieldDecoderBuilder.put(columnHandle, fieldDecoder);
            }
        }

        ImmutableList<KinesisColumnHandle> handles = handleBuilder.build();
        ImmutableMap<KinesisColumnHandle, KinesisFieldDecoder<?>> messageFieldDecoders = messageFieldDecoderBuilder.build();

        return new KinesisRecordSet(kinesisSplit, clientManager, handles, messageDecoder,
                messageFieldDecoders, kinesisConnectorConfig,
                getIteratorType(session),
                isCheckpointEnabled(session),
                getCheckpointIterationNumber(session),
                getCheckpointLogicalName(session));
    }
}
