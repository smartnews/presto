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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public final class AwsFunctions
{
    private static final AmazonDynamoDBClient DDB_CLIENT;
    private static final DynamoDB DDB;
    private static final LoadingCache<String, Table> TABLE_CACHE;

    static {
        ClientConfiguration awsConfig = new ClientConfiguration();
        awsConfig.setUseTcpKeepAlive(true);
        awsConfig.setProtocol(Protocol.HTTP);
        DDB_CLIENT = new AmazonDynamoDBAsyncClient(
                new DefaultAWSCredentialsProviderChain(), awsConfig);

        DDB_CLIENT.configureRegion(Regions.AP_NORTHEAST_1);
        DDB = new DynamoDB(DDB_CLIENT);

        TABLE_CACHE = CacheBuilder.newBuilder()
                .concurrencyLevel(10)
                .maximumSize(100)
                .expireAfterAccess(60, TimeUnit.SECONDS)
                .build(new CacheLoader<String, Table>()
                {
                    @Override
                    public Table load(String tbl)
                            throws Exception
                    {
                        return DDB.getTable(tbl);
                    }
                });
    }

    private AwsFunctions()
    {
    }

    private static Slice ddbGet(Slice tbl, KeyAttribute... k)
    {
        String t = tbl.toStringUtf8();
        try {
            Table table = TABLE_CACHE.get(t);

            Index x = table.getIndex("x");
            if (table != null) {
                Item item = table.getItem(k);
                if (item != null) {
                    return Slices.utf8Slice(item.toJSON());
                }
            }
        }
        catch (ExecutionException e) {
        }
        return null;
    }

    @Description("DynamoDB get")
    @ScalarFunction("ddb_get")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice ddbGet(@SqlType(StandardTypes.VARCHAR) Slice tbl,
            @SqlType(StandardTypes.VARCHAR) Slice hashKeyName,
            @SqlType(StandardTypes.VARCHAR) Slice hashKeyValue)
    {
        return ddbGet(tbl,
                new KeyAttribute(hashKeyName.toStringUtf8(), hashKeyValue.toStringUtf8()));
    }

    @Description("DynamoDB get")
    @ScalarFunction("ddb_get")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice ddbGet(@SqlType(StandardTypes.VARCHAR) Slice tbl,
            @SqlType(StandardTypes.VARCHAR) Slice hashKeyName,
            @SqlType(StandardTypes.BIGINT) long hashKeyValue)
    {
        return ddbGet(tbl,
                new KeyAttribute(hashKeyName.toStringUtf8(), Long.valueOf(hashKeyValue)));
    }

    @Description("DynamoDB get")
    @ScalarFunction("ddb_get")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice ddbGet(@SqlType(StandardTypes.VARCHAR) Slice tbl,
            @SqlType(StandardTypes.VARCHAR) Slice hashKeyName,
            @SqlType(StandardTypes.VARCHAR) Slice hashKeyValue,
            @SqlType(StandardTypes.VARCHAR) Slice rangeKeyName,
            @SqlType(StandardTypes.VARCHAR) Slice rangeKeyValue)
    {
        return ddbGet(tbl,
                new KeyAttribute(hashKeyName.toStringUtf8(), hashKeyValue.toStringUtf8()),
                new KeyAttribute(rangeKeyName.toStringUtf8(), rangeKeyValue.toStringUtf8()));
    }

    @Description("DynamoDB get")
    @ScalarFunction("ddb_get")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice ddbGet(@SqlType(StandardTypes.VARCHAR) Slice tbl,
            @SqlType(StandardTypes.VARCHAR) Slice hashKeyName,
            @SqlType(StandardTypes.VARCHAR) Slice hashKeyValue,
            @SqlType(StandardTypes.VARCHAR) Slice rangeKeyName,
            @SqlType(StandardTypes.BIGINT) long rangeKeyValue)
    {
        return ddbGet(tbl,
                new KeyAttribute(hashKeyName.toStringUtf8(), hashKeyValue.toStringUtf8()),
                new KeyAttribute(rangeKeyName.toStringUtf8(), Long.valueOf(rangeKeyValue)));
    }

    private static Slice ddbGetGsi(Slice tbl, Slice idx, QuerySpec spec)
    {
        String t = tbl.toStringUtf8();
        try {
            Table table = TABLE_CACHE.get(t);

            if (table != null) {
                Index index = table.getIndex(idx.toStringUtf8());
                ItemCollection<QueryOutcome> items = index.query(
                        spec.withMaxResultSize(1));
                if (items != null) {
                    Iterator<Item> iter = items.iterator();
                    if (iter.hasNext()) {
                        return Slices.utf8Slice(iter.next().toJSON());
                    }
                }
            }
        }
        catch (ExecutionException e) {
        }
        return null;
    }

    @Description("DynamoDB get using Global Secondary Index")
    @ScalarFunction("ddb_get_gsi")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice ddbGetGsi(@SqlType(StandardTypes.VARCHAR) Slice tbl,
            @SqlType(StandardTypes.VARCHAR) Slice idx,
            @SqlType(StandardTypes.VARCHAR) Slice hashKeyName,
            @SqlType(StandardTypes.VARCHAR) Slice hashKeyValue)
    {
        return ddbGetGsi(tbl, idx,
                new QuerySpec().withHashKey(
                        new KeyAttribute(hashKeyName.toStringUtf8(), hashKeyValue.toStringUtf8())));
    }

    @Description("DynamoDB get using Global Secondary Index")
    @ScalarFunction("ddb_get_gsi")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice ddbGetGsi(@SqlType(StandardTypes.VARCHAR) Slice tbl,
            @SqlType(StandardTypes.VARCHAR) Slice idx,
            @SqlType(StandardTypes.VARCHAR) Slice hashKeyName,
            @SqlType(StandardTypes.BIGINT) long hashKeyValue)
    {
        return ddbGetGsi(tbl, idx,
                new QuerySpec().withHashKey(
                        new KeyAttribute(hashKeyName.toStringUtf8(), Long.valueOf(hashKeyValue))));
    }
}
