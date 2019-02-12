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

import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.metadata.TableHandle;
import io.prestosql.plugin.kinesis.util.EmbeddedKinesisStream;
import io.prestosql.plugin.kinesis.util.TestUtils;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.type.BigintType;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.tests.StandaloneQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static io.prestosql.plugin.kinesis.util.TestUtils.createEmptyStreamDescription;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMinimalFunctionality
{
    private static final Logger log = Logger.get(TestMinimalFunctionality.class);

    private static final Session SESSION = Session.builder(new SessionPropertyManager())
            .setIdentity(new Identity("test", Optional.empty()))
            .setSource("source")
            .setCatalog("kinesis")
            .setSchema("default")
            .setTimeZoneKey(UTC_KEY)
            .setLocale(ENGLISH)
            .build();

    private EmbeddedKinesisStream embeddedKinesisStream;
    private String streamName;
    private StandaloneQueryRunner queryRunner;

    @Parameters({
            "kinesis.awsAccessKey",
            "kinesis.awsSecretKey"
    })
    @BeforeClass
    public void start(String accessKey, String secretKey)
            throws Exception
    {
        embeddedKinesisStream = new EmbeddedKinesisStream(accessKey, secretKey);
    }

    @AfterClass
    public void stop()
            throws Exception
    {
        embeddedKinesisStream.close();
    }

    @Parameters({
            "kinesis.awsAccessKey",
            "kinesis.awsSecretKey"
    })
    @BeforeMethod
    public void spinUp(String accessKey, String secretKey)
            throws Exception
    {
        streamName = "test_" + UUID.randomUUID().toString().replaceAll("-", "_");
        embeddedKinesisStream.createStream(2, streamName);
        this.queryRunner = new StandaloneQueryRunner(SESSION);
        TestUtils.installKinesisPlugin(embeddedKinesisStream, queryRunner,
                ImmutableMap.<SchemaTableName, KinesisStreamDescription>builder().put(createEmptyStreamDescription(streamName,
                        new SchemaTableName("default", streamName))).build(),
                accessKey, secretKey);
    }

    private void createMessages(String streamName, int count)
            throws Exception
    {
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(streamName);
        List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
            putRecordsRequestEntry.setData(ByteBuffer.wrap(UUID.randomUUID().toString().getBytes()));
            putRecordsRequestEntry.setPartitionKey(Long.toString(i));
            putRecordsRequestEntryList.add(putRecordsRequestEntry);
        }

        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        embeddedKinesisStream.getKinesisClient().putRecords(putRecordsRequest);
    }

    @Test
    public void testStreamExists()
            throws Exception
    {
        QualifiedObjectName name = new QualifiedObjectName("kinesis", "default", streamName);
        Optional<TableHandle> handle = queryRunner.getServer().getMetadata().getTableHandle(SESSION, name);
        assertTrue(handle.isPresent());
    }

    @Test
    public void testStreamHasData()
            throws Exception
    {
        MaterializedResult result = queryRunner.execute("Select count(1) from " + streamName);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(0)
                .build();

        assertEquals(result, expected);

        int count = 500;
        createMessages(streamName, count);

        result = queryRunner.execute("SELECT count(1) from " + streamName);

        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(count)
                .build();

        assertEquals(result, expected);
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        embeddedKinesisStream.delteStream(streamName);
        queryRunner.close();
    }
}
