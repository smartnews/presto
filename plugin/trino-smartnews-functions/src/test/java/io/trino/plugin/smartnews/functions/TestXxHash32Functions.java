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

import io.trino.Session;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;

public class TestXxHash32Functions
{
    private LocalQueryRunner runner;

    @BeforeClass
    public void init()
    {
        runner = LocalQueryRunner.create(testSessionBuilder().build());
        runner.installPlugin(new SmartnewsFunctionsPlugin());
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        if (runner != null) {
            runner.close();
            runner = null;
        }
    }

    // ---------- NULL tests ----------

    @Test
    public void testNull()
    {
        assertNull(runner.execute("SELECT xxhash32(CAST(NULL AS varchar))").getOnlyValue());
        assertNull(runner.execute("SELECT xxhash32(CAST(NULL AS varchar), 1)").getOnlyValue());
    }

    // ---------- typeof(xxhash32(...)) ----------

    @DataProvider(name = "typeProvider")
    public Object[][] typeProvider()
    {
        return new Object[][] {
                {"'abc'", "integer"},
                {"CAST('abc' AS varchar(3))", "integer"},
                {"CAST('abc' AS char(5))", "integer"},
                {"to_utf8('abc')", "integer"},
                {"CAST(123 AS INTEGER)", "integer"},   // second parameter still dtype integer
        };
    }

    // ---------- deterministic (same session) ----------

    @Test
    public void testDeterministicInSameSession()
    {
        Session session = testSessionBuilder().build();

        assertEquals(
                runner.execute(session, "SELECT xxhash32('abc', 1)").getOnlyValue(),
                runner.execute(session, "SELECT xxhash32('abc', 1)").getOnlyValue());
    }

    // ---------- seed effect ----------

    @Test
    public void testSeedEffect()
    {
        Object h1 = runner.execute("SELECT xxhash32('abc', 0)").getOnlyValue();
        Object h2 = runner.execute("SELECT xxhash32('abc', 1)").getOnlyValue();
        Object h3 = runner.execute("SELECT xxhash32('abc', 2)").getOnlyValue();

        assertNotEquals(h1, h2);
        assertNotEquals(h2, h3);
    }

    // ---------- UTF-8 correctness ----------

    @Test
    public void testUTF8()
    {
        Session session = testSessionBuilder().build();

        assertEquals(
                runner.execute(session, "SELECT xxhash32('測試', 123)").getOnlyValue(),
                runner.execute(session, "SELECT xxhash32('測試', 123)").getOnlyValue());

        assertNotEquals(
                runner.execute(session, "SELECT xxhash32('測試', 1)").getOnlyValue(),
                runner.execute(session, "SELECT xxhash32('測試', 2)").getOnlyValue());
    }

    // ---------- fixed value regression test ----------
    @Test
    public void testFixedRegression()
    {
        assertEquals(
                runner.execute("SELECT abs(xxhash32('test_uuid', 20251027)) % 100").getOnlyValue(),
                67);
    }
}
