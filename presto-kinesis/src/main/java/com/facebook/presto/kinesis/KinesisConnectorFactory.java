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
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * This factory class creates the KinesisConnector during server start and binds all the dependency
 * by calling create() method.
 */
public class KinesisConnectorFactory
        implements ConnectorFactory
{
    private TypeManager typeManager;
    private Optional<Supplier<Map<SchemaTableName, KinesisStreamDescription>>> tableDescriptionSupplier = Optional.empty();
    private Map<String, String> optionalConfig = ImmutableMap.of();

    KinesisConnectorFactory(TypeManager typeManager,
                            Optional<Supplier<Map<SchemaTableName, KinesisStreamDescription>>> tableDescriptionSupplier,
                            Map<String, String> optionalConfig)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.optionalConfig = requireNonNull(optionalConfig, "optionalConfig is null");
        this.tableDescriptionSupplier = requireNonNull(tableDescriptionSupplier, "tableDescriptionSupplier is null");
    }

    @Override
    public String getName()
    {
        return "kinesis";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new KinesisHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(config, "config is null");

        try {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new KinesisConnectorModule(),
                    new Module()
                    {
                        @Override
                        public void configure(Binder binder)
                        {
                            binder.bindConstant().annotatedWith(Names.named("connectorId")).to(connectorId);
                            binder.bind(TypeManager.class).toInstance(typeManager);

                            if (tableDescriptionSupplier.isPresent()) {
                                binder.bind(new TypeLiteral<Supplier<Map<SchemaTableName, KinesisStreamDescription>>>()
                                {
                                }).toInstance(tableDescriptionSupplier.get());
                            }
                            else {
                                binder.bind(new TypeLiteral<Supplier<Map<SchemaTableName, KinesisStreamDescription>>>()
                                {
                                }).to(KinesisTableDescriptionSupplier.class).in(Scopes.SINGLETON);
                            }
                        }
                    }
            );

            Injector injector = app.strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            return injector.getInstance(KinesisConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
