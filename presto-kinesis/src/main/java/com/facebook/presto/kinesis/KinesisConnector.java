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
package com.facebook.presto.kinesis;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class KinesisConnector
        implements Connector
{
    private final KinesisMetadata metadata;
    private final KinesisSplitManager splitManager;
    private final KinesisRecordSetProvider recordSetProvider;
    private final KinesisHandleResolver handleResolver;
    private final KinesisSessionProperties kinesisSessionProperties;

    @Inject
    public KinesisConnector(
            KinesisHandleResolver handleResolver,
            KinesisMetadata metadata,
            KinesisSplitManager splitManager,
            KinesisRecordSetProvider recordSetProvider,
            KinesisSessionProperties kinesisSessionProperties)
    {
        this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
        this.kinesisSessionProperties = requireNonNull(kinesisSessionProperties, "kinesisSessionProperties is null");
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return handleResolver;
    }

    @Override
    public ConnectorMetadata getMetadata()
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return recordSetProvider;
    }

    @Override
    public ConnectorRecordSinkProvider getRecordSinkProvider()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorIndexResolver getIndexResolver()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return kinesisSessionProperties.getSessionProperties();
    }
}
