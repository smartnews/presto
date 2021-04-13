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

import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.smartnews.event.QueryEventLoggerFactory;
import io.prestosql.plugin.smartnews.functions.aggregation.HyperLogLogMergeAggregation;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.eventlistener.EventListenerFactory;
import io.prestosql.spi.type.TypeManager;

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
                .add(HyperLogLogMergeAggregation.class)
                .add(HyperLogLogFunctions.class)
                .add(HttpFunctions.class)
                .add(AwsFunctions.class)
                .add(AssertFunctions.class)
                .add(ExtendedFunctions.class)
                .add(ExtendedMapFunctions.class)
                .build();
    }

    @Override
    public Iterable<EventListenerFactory> getEventListenerFactories()
    {
        return ImmutableSet.of(new QueryEventLoggerFactory());
    }
}
