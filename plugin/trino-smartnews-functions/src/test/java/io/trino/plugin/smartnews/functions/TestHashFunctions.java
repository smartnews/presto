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
import io.trino.Session;
import io.trino.spi.Plugin;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.TestingSession;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Set;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;

public class TestHashFunctions
{
    private LocalQueryRunner runner;

    @BeforeClass
    public void init()
    {
        runner = LocalQueryRunner.create(testSessionBuilder().build());
        runner.installPlugin(new Plugin() {
            @Override
            public Set<Class<?>> getFunctions()
            {
                return ImmutableSet.<Class<?>>builder()
                        .add(HashFunctions.class)
                        .build();
            }
        });
        HashFunctions.setSnHashKey("test_sn_hash_key");
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        if (runner != null) {
            runner.close();
            runner = null;
        }
    }

    @Test
    public void testHashConsistency() throws Exception
    {
        Session session = TestingSession.testSessionBuilder().build();
        assertNull(runner.execute("select sn_hash(NULL)").getOnlyValue());
        assertEquals(runner.execute(session, "select sn_hash('123')").getOnlyValue(), runner.execute(session, "select sn_hash('123')").getOnlyValue());
        assertEquals(runner.execute(session, "select sn_hash('測試')").getOnlyValue(), runner.execute(session, "select sn_hash('測試')").getOnlyValue());
        assertEquals(runner.execute(session, "select sn_hash(123)").getOnlyValue(), runner.execute(session, "select sn_hash(123)").getOnlyValue());
        assertEquals(runner.execute(session, "select sn_hash(0.123)").getOnlyValue(), runner.execute(session, "select sn_hash(0.123)").getOnlyValue());
        assertEquals(runner.execute(session, "select sn_hash('abc')").getOnlyValue(), runner.execute(session, "select sn_hash('abc')").getOnlyValue());
        assertNotSame(runner.execute(session, "select sn_hash('123')").getOnlyValue(), runner.execute(session, "select sn_hash(123)").getOnlyValue());
        assertNotSame(runner.execute(session, "select sn_hash('123')").getOnlyValue(), "123");
        assertNotSame(runner.execute(session, "select sn_hash('')").getOnlyValue(), runner.execute(session, "select sn_hash(' ')").getOnlyValue());
    }

    @Test
    public void testDynamicHashing() throws Exception
    {
        Session session = TestingSession.testSessionBuilder().build();
        String prevRes = runner.execute(session, "select sn_hash('123')").getOnlyValue().toString();
        init();
        assertNotSame(runner.execute(session, "select sn_hash('123')").getOnlyValue(), prevRes);
    }

    @Test
    public void testHashingUTF8() throws Exception
    {
        String prevRes = runner.execute("select sn_hash('測試')").getOnlyValue().toString();
        System.out.println(prevRes);
        init();
        assertNotSame(runner.execute("select sn_hash('測試')").getOnlyValue(), prevRes);
    }

    @Test
    public void testHashingPrefixAndLength() throws Exception
    {
        assertEquals(runner.execute("select sn_hash('測試')").getOnlyValue().toString().substring(0, 2), HashFunctions.SN_HASH_PREFIX);
        assertEquals(runner.execute("select sn_hash('測試')").getOnlyValue().toString().length(), HashFunctions.SN_HASH_LENGTH);
    }

    @Test
    public void testmHash() throws Exception
    {
        Object ret = runner.execute("select mhash('123')").getOnlyValue();
        assertEquals(2405477, (int) ret);

        ret = runner.execute("select mhash('123', 1)").getOnlyValue();
        assertEquals(1, (int) ret);

        ret = runner.execute("select mhash('123', 2)").getOnlyValue();
        assertEquals(1, (int) ret);

        ret = runner.execute("select mhash(null)").getOnlyValue();
        assertEquals(null, ret);

        ret = runner.execute("select mhash('')").getOnlyValue();
        assertEquals(11977257, (int) ret);

        ret = runner.execute("select mhash('', 1999)").getOnlyValue();
        assertEquals(1463, (int) ret);
    }

    @Test
    public void testmHashArray() throws Exception
    {
        Object ret = runner.execute("select mhash(array['123'])").getOnlyValue();
        assertEquals(2405477, (int) ret);

        ret = runner.execute("select mhash(array[])").getOnlyValue();
        assertEquals(1, (int) ret);

        ret = runner.execute("select mhash(array['123', 'abc'])").getOnlyValue();
        assertEquals(1992388, (int) ret);

        ret = runner.execute("select mhash(array['123', 'abc', null])").getOnlyValue();
        assertEquals(10771779, (int) ret);
    }

    @Test
    public void testSimHash() throws Exception
    {
        Object ret = runner.execute("select to_hex(simhash(map(array['123'], array[double '1.23'])))").getOnlyValue();
        assertEquals("A4F8344E8A95BB1041FD56CD77AF52C1", (String) ret);
    }
}
