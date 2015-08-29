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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringSessionProperty;

public final class KinesisSessionProperties
{
    private static final String CHECKPOINT_ENABLED = "checkpoint_enabled";
    private static final String CHECKPOINT_ITERATION_NUMBER = "checkpoint_iteration_number";
    private static final String CHECKPOINT_LOGICAL_NAME = "checkpoint_logical_name";
    private static final String ITERATOR_TYPE = "iterator_type";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public KinesisSessionProperties(KinesisConnectorConfig config)
    {
        sessionProperties = ImmutableList.of(
                booleanSessionProperty(
                        CHECKPOINT_ENABLED,
                        "should use checkpoint when access kinesis",
                        false,
                        false),
                integerSessionProperty(
                        CHECKPOINT_ITERATION_NUMBER,
                        "checkpoint iteration number",
                        0,
                        false),
                stringSessionProperty(
                        CHECKPOINT_LOGICAL_NAME,
                        "checkpoint logical name",
                        "",
                        false),
                stringSessionProperty(
                        ITERATOR_TYPE,
                        "kinesis stream iterator type",
                        "TRIM_HORIZON",
                        false)
        );
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isCheckpointEnabled(ConnectorSession session)
    {
        return session.getProperty(CHECKPOINT_ENABLED, Boolean.class);
    }

    public static int getCheckpointIterationNumber(ConnectorSession session)
    {
        return session.getProperty(CHECKPOINT_ITERATION_NUMBER, Integer.class);
    }

    public static String getCheckpointLogicalName(ConnectorSession session)
    {
        return session.getProperty(CHECKPOINT_LOGICAL_NAME, String.class);
    }

    public static String getIteratorType(ConnectorSession session)
    {
        return session.getProperty(ITERATOR_TYPE, String.class);
    }
}
