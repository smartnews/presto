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
import static org.testng.Assert.assertTrue;

public class TestHashFunctions
{
    private LocalQueryRunner runner;

    @BeforeClass
    public void init()
    {
        runner = LocalQueryRunner.create(testSessionBuilder().build());
        runner.installPlugin(new SmartnewsFunctionsPlugin());
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
    public void testHashNull() throws Exception
    {
        assertNull(runner.execute("select sn_hash(cast(NULL as varchar))").getOnlyValue());
        assertNull(runner.execute("select sn_hash(cast(NULL as int))").getOnlyValue());
        assertNull(runner.execute("select sn_hash(cast(NULL as double))").getOnlyValue());
        assertNull(runner.execute("select sn_hash(a) from (values (1), (NULL)) as t(a)").getMaterializedRows().get(1).getField(0));
        assertNull(runner.execute("select sn_hash(a) from (values (1.0), (NULL)) as t(a)").getMaterializedRows().get(1).getField(0));
        assertNull(runner.execute("select sn_hash(a) from (values ('abc'), (NULL)) as t(a)").getMaterializedRows().get(1).getField(0));
    }

    @DataProvider(name = "testHashVariantTypesProvider")
    public Object[][] testHashVariantTypesProvider()
    {
        return new Object[][] {
            {"1", "integer"},
            {"CAST(1 AS BIGINT)", "integer"},
            {"CAST(1 AS DOUBLE)", "double"},
            {"CAST(1 AS REAL)", "double"},
            {"'aa'", "varchar"},
            {"CAST('aa' AS VARCHAR(1))", "varchar"},
            {"CAST('aa' AS CHAR)", "char(1)"},
            {"CAST('aa' AS CHAR(10))", "char(10)"}
        };
    }

    @Test(dataProvider = "testHashVariantTypesProvider")
    public void testHashVariantTypes(String val, String type) throws Exception
    {
        // System.out.println(String.format("sn_hash(%s) = %s", val, runner.execute(String.format("select sn_hash(%s)", val)).getOnlyValue()));
        String realType = runner.execute(String.format("select typeof(sn_hash(%s))", val)).getOnlyValue().toString();
        assertEquals(realType, type, String.format("Type check failed for sn_hash(%s)", val, type, realType));
    }

    @Test
    public void testHashConsistency() throws Exception
    {
        Session session = testSessionBuilder().build();
        assertEquals(runner.execute(session, "select sn_hash('123')").getOnlyValue(), runner.execute(session, "select sn_hash('123')").getOnlyValue());
        assertEquals(runner.execute(session, "select sn_hash(123)").getOnlyValue(), runner.execute(session, "select sn_hash(123)").getOnlyValue());
        assertEquals(runner.execute(session, "select sn_hash(0.123)").getOnlyValue(), runner.execute(session, "select sn_hash(0.123)").getOnlyValue());
        assertEquals(runner.execute(session, "select sn_hash('abc')").getOnlyValue(), runner.execute(session, "select sn_hash('abc')").getOnlyValue());
        assertNotEquals(runner.execute(session, "select sn_hash('123')").getOnlyValue(), runner.execute(session, "select sn_hash(123)").getOnlyValue());
        assertNotEquals(runner.execute(session, "select sn_hash('123')").getOnlyValue(), "123");
        assertNotEquals(runner.execute(session, "select sn_hash('')").getOnlyValue(), runner.execute(session, "select sn_hash(' ')").getOnlyValue());
        assertNotEquals(runner.execute(session, "select sn_hash('aa')").getOnlyValue(), runner.execute(session, "select sn_hash(' aa')").getOnlyValue());
        assertNotEquals(runner.execute(session, "select sn_hash('aa')").getOnlyValue(), runner.execute(session, "select sn_hash('aa ')").getOnlyValue());
    }

    @DataProvider(name = "testHashVarcharsProvider")
    public Object[][] testHashVarcharsProvider()
    {
        return new Object[][] {
            {"'123'", "'123'"},
            {"cast('1234' as varchar(3))", "'123'"},
            {"cast('  ' as varchar(1))", "' '"},
            {"cast(' ' as varchar(1))", "' '"},
        };
    }

    @Test(dataProvider = "testHashVarcharsProvider")
    public void testHashVarchars(String val1, String val2) throws Exception
    {
        assertEquals(
                runner.execute(String.format("select sn_hash(%s)", val1)).getOnlyValue(),
                runner.execute(String.format("select sn_hash(%s)", val2)).getOnlyValue());
    }

    @DataProvider(name = "testHashCharsProvider")
    public Object[][] testHashCharsProvider()
    {
        return new Object[][] {
            {"cast('123' as char(3))", "'123'", 3},
            {"cast('1234' as char(3))", "'123'", 3},
            {"cast('.' as char(1))", "'.'", 1},
            {"cast('.' as char)", "'.'", 1},
            {"cast('..' as char(2))", "cast('...' as char(2))", 2},
        };
    }

    @Test(dataProvider = "testHashCharsProvider")
    public void testHashChars(String val1, String val2, int len) throws Exception
    {
        assertEquals(
                runner.execute(String.format("select sn_hash(%s)", val1)).getOnlyValue(),
                runner.execute(String.format("select cast(sn_hash(%s) as char(%d))", val2, len)).getOnlyValue());
    }

    @Test
    public void testDynamicHashing() throws Exception
    {
        assertNotEquals(
                runner.execute(testSessionBuilder().build(), "select sn_hash('123')"),
                runner.execute(testSessionBuilder().build(), "select sn_hash('123')"));
        assertNotEquals(
                runner.execute(testSessionBuilder().build(), "select sn_hash(123)"),
                runner.execute(testSessionBuilder().build(), "select sn_hash(123)"));
        assertNotEquals(
                runner.execute(testSessionBuilder().build(), "select sn_hash(0.123)"),
                runner.execute(testSessionBuilder().build(), "select sn_hash(0.123)"));
    }

    @Test
    public void testHashingUTF8() throws Exception
    {
        assertEquals(
                runner.execute("select sn_hash('測試')").getOnlyValue(),
                runner.execute("select sn_hash('測試')").getOnlyValue());
        assertNotEquals(
                runner.execute(testSessionBuilder().build(), "select sn_hash('測試')").getOnlyValue(),
                runner.execute(testSessionBuilder().build(), "select sn_hash('測試')").getOnlyValue());
    }

    @Test
    public void testHashingPrefixAndLength() throws Exception
    {
        assertEquals(runner.execute("select sn_hash('測試')").getOnlyValue().toString().substring(0, 2), HashFunctions.SN_HASH_PREFIX);
        assertEquals(runner.execute("select sn_hash('測試')").getOnlyValue().toString().length(), HashFunctions.SN_HASH_LENGTH);
    }

    @Test
    public void testHashingNumericalShouldBeNonNegative() throws Exception
    {
        assertTrue((int) runner.execute("select sn_hash(1)").getOnlyValue() <= 0);
        assertTrue((int) runner.execute("select sn_hash(12)").getOnlyValue() <= 0);
        assertTrue((int) runner.execute("select sn_hash(123)").getOnlyValue() <= 0);
        assertTrue((int) runner.execute("select sn_hash(2147483647)").getOnlyValue() <= 0);
        assertTrue((int) runner.execute("select sn_hash(-1)").getOnlyValue() <= 0);
        assertTrue((int) runner.execute("select sn_hash(-12)").getOnlyValue() <= 0);
        assertTrue((int) runner.execute("select sn_hash(-123)").getOnlyValue() <= 0);
        assertTrue((int) runner.execute("select sn_hash(-2147483648)").getOnlyValue() <= 0);
        assertTrue((double) runner.execute("select sn_hash(123.0)").getOnlyValue() <= 0);
        assertTrue((double) runner.execute("select sn_hash(12.3)").getOnlyValue() <= 0);
        assertTrue((double) runner.execute("select sn_hash(1.23)").getOnlyValue() <= 0);
        assertTrue((double) runner.execute("select sn_hash(-123.0)").getOnlyValue() <= 0);
        assertTrue((double) runner.execute("select sn_hash(-12.3)").getOnlyValue() <= 0);
        assertTrue((double) runner.execute("select sn_hash(-1.23)").getOnlyValue() <= 0);
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
