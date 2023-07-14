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
package io.trino.plugin.smartnews.functions.aggregation;

import io.trino.operator.scalar.AbstractTestFunctions;
import io.trino.plugin.smartnews.functions.SmartnewsFunctionsPlugin;
import io.trino.sql.query.QueryAssertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import static org.assertj.core.api.Assertions.assertThat;

public class TestMapWeightedCountAggregation
        extends AbstractTestFunctions
{
    private QueryAssertions assertions;

    @BeforeClass
    public void setUp()
    {
        assertions = new QueryAssertions();
        assertions.addPlugin(new SmartnewsFunctionsPlugin());
    }

    @AfterClass
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @org.testng.annotations.Test
    public void testNull()
    {
        assertThat(assertions.query("SELECT map_weighted_count(k, v) FROM (VALUES (cast(null as varchar), cast(null as double))) t(k, v)"))
                .matches("VALUES cast(map(array[], array[]) as map(varchar, double))");

        assertThat(assertions.query("SELECT map_weighted_count(k, v) FROM (VALUES (null, 2.0)) t(k, v)"))
                .matches("VALUES cast(map(array[], array[]) as map(varchar, double))");

//        assertThat(assertions.query("SELECT map_weighted_count(k, v) FROM (VALUES (varchar '1', cast(null as double))) t(k, v)"))
//                .matches("VALUES map(array[varchar '1'], array[double '0.1'])");
    }

    @org.testng.annotations.Test
    public void testDouble()
    {
        assertThat(assertions.query("SELECT map_weighted_count(k, v) FROM (VALUES ('1', 1.0), ('1', 2.0), ('2', 3.5)) t(k, v)"))
                .matches("VALUES map(array[cast('1' as varchar), cast('2' as varchar)], array[cast(3.0 as double), cast(3.5 as double)])");
    }

    @org.testng.annotations.Test
    public void testDoubleGroupby()
    {
        // group by
        assertThat(assertions.query("SELECT map_weighted_count(k, v) FROM (VALUES ('1', 1.0), ('1', 2.0), ('2', 3.5)) t(k, v) GROUP BY k"))
                .matches("VALUES map(array[cast('1' as varchar)], array[cast(3.0 as double)])," +
                        "map(array[cast('2' as varchar)], array[cast(3.5 as double)])");
    }

    @org.testng.annotations.Test
    public void testMap()
    {
        assertThat(assertions.query("SELECT map_weighted_count(m, 2.0) FROM (VALUES (map(array['1', '2'], array[1.0, 2.0])), (map(array['2'], array[4.0]))) t(m)"))
                .matches("VALUES map(array[cast('1' as varchar), cast('2' as varchar)], array[cast(2.0 as double), cast(12.0 as double)])");
    }

    @org.testng.annotations.Test
    public void testMapGroupby()
    {
        // group by
        String sql = "SELECT k, map_weighted_count(m, 2.0) FROM (" +
                " VALUES " +
                "   ('a', map(array['1', '2'], array[1.0, 2.0]))" +
                "   , ('b', map(array['2'], array[4.0]))" +
                ") t(k, m) GROUP BY k";
        String exp = "VALUES " +
                "   ('a', map(array[cast('1' as varchar), cast('2' as varchar)], array[cast(2.0 as double), cast(4.0 as double)]))" +
                "   , ('b', map(array[cast('2' as varchar)], array[cast(8.0 as double)]))";
        assertThat(assertions.query(sql)).matches(exp);
    }

    @org.testng.annotations.Test
    public void testArray()
    {
        assertThat(assertions.query("SELECT map_weighted_count(m, 2.0) FROM (VALUES (array['1', '2']), (array['2'])) t(m)"))
                .matches("VALUES map(array[cast('1' as varchar), cast('2' as varchar)], array[cast(2.0 as double), cast(4.0 as double)])");
    }

    @org.testng.annotations.Test
    public void testArrayGroupby()
    {
        // group by
        String sql = "SELECT k, map_weighted_count(m, 2.0) FROM (" +
                " VALUES " +
                "   ('a', array['1', '2', '2'])" +
                "   , ('b', array['1', '2'])" +
                "   , ('b', array['1'])" +
                ") t(k, m) GROUP BY k";
        String exp = "VALUES " +
                "   ('a', map(array[cast('1' as varchar), cast('2' as varchar)], array[cast(2.0 as double), cast(4.0 as double)]))" +
                "   , ('b', map(array[cast('1' as varchar), cast('2' as varchar)], array[cast(4.0 as double), cast(2.0 as double)]))";
        assertThat(assertions.query(sql)).matches(exp);
    }
}
