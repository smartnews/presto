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

import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.VarcharType;
import io.trino.sql.query.QueryAssertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.HashMap;

import static io.trino.util.StructuralTestUtil.mapType;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMapFunctions
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
    public void testMapNormBasic()
    {
        HashMap<String, Double> expected = new HashMap<>();
        expected.put("a", 0.2);
        expected.put("b", 0.8);
        assertThat(assertions.function("map_norm", "map(array['a','b'], array[1.0, 4.0])"))
                .hasType(mapType(VarcharType.VARCHAR, DoubleType.DOUBLE))
                .isEqualTo(expected);
    }

    @org.testng.annotations.Test
    public void testMapNormBasicBigint()
    {
        HashMap<String, Double> expected = new HashMap<>();
        expected.put("a", 0.4);
        expected.put("b", 0.6);
        assertThat(assertions.function("map_norm", "map(array['a','b'], array[bigint '2', bigint '3'])"))
                .hasType(mapType(VarcharType.VARCHAR, DoubleType.DOUBLE))
                .isEqualTo(expected);
    }

    @org.testng.annotations.Test
    public void testMapNormNull()
    {
        assertThat(assertions.function("map_norm", "null"))
                .isNull(mapType(VarcharType.VARCHAR, DoubleType.DOUBLE));
    }

    @org.testng.annotations.Test
    public void testString2MapBasic()
    {
        HashMap<String, String> expected = new HashMap<>();
        expected.put("a", "1.0");
        expected.put("b", "4.0");
        assertThat(assertions.function("str_to_map", "'a:1.0,b:4.0'"))
                .hasType(mapType(VarcharType.VARCHAR, VarcharType.VARCHAR))
                .isEqualTo(expected);

        assertThat(assertions.function("str_to_map", "'a:1.0,b:4.0'", "','", "':'"))
                .hasType(mapType(VarcharType.VARCHAR, VarcharType.VARCHAR))
                .isEqualTo(expected);

        assertThat(assertions.function("str_to_map", "'a:1.0;b:4.0'", "';'", "':'"))
                .hasType(mapType(VarcharType.VARCHAR, VarcharType.VARCHAR))
                .isEqualTo(expected);

        // duplicated, keep the last one
        assertThat(assertions.function("str_to_map", "'a:2.0;b:4.0;a:1.0'", "';'", "':'"))
                .hasType(mapType(VarcharType.VARCHAR, VarcharType.VARCHAR))
                .isEqualTo(expected);

        HashMap<String, String> expected2 = new HashMap<>();
        expected2.put("", null);
        assertThat(assertions.function("str_to_map", "''"))
                .hasType(mapType(VarcharType.VARCHAR, VarcharType.VARCHAR))
                .isEqualTo(expected2);

        HashMap<String, String> expected3 = new HashMap<>();
        expected3.put("a", null);
        assertThat(assertions.function("str_to_map", "'a:'", "','", "':'"))
                .hasType(mapType(VarcharType.VARCHAR, VarcharType.VARCHAR))
                .isEqualTo(expected3);
    }

    @org.testng.annotations.Test
    public void testString2MapNull()
    {
        assertThat(assertions.function("str_to_map", "null"))
                .hasType(mapType(VarcharType.VARCHAR, VarcharType.VARCHAR))
                .isNull();

        HashMap<String, String> expected = new HashMap<>();
        expected.put("a", "1.0");
        expected.put("b", "4.0");

        assertThat(assertions.function("str_to_map", "'a:1.0,b:4.0'", "null", "':'"))
                .hasType(mapType(VarcharType.VARCHAR, VarcharType.VARCHAR))
                .isEqualTo(expected);

        assertThat(assertions.function("str_to_map", "'a:1.0,b:4.0'", "','", "null"))
                .hasType(mapType(VarcharType.VARCHAR, VarcharType.VARCHAR))
                .isEqualTo(expected);

        assertThat(assertions.function("str_to_map", "'a:1.0,b:4.0'", "null", "null"))
                .hasType(mapType(VarcharType.VARCHAR, VarcharType.VARCHAR))
                .isEqualTo(expected);
    }

    @org.testng.annotations.Test
    public void testRemapLibsvm()
    {
        assertThat(assertions.function("remap_libsvm", "'2:1.0 3:4.5'", "integer '2400'", "'dummy'"))
                .hasType(VarcharType.VARCHAR)
                .isEqualTo("2402:1 2403:4.5");
    }

    @org.testng.annotations.Test
    public void mapTopKDouble()
    {
        HashMap<String, Double> expected = new HashMap<>();
        expected.put("c", (Double) 3.0);
        expected.put("b", (Double) 4.0);
        assertThat(assertions.function("map_topk", "map(array[varchar 'a',varchar 'b',varchar 'c',varchar 'd'], array[double '1.0', double '4.0', double '3.0', double '2.0'])", "2"))
                .hasType(mapType(VarcharType.VARCHAR, DoubleType.DOUBLE))
                .isEqualTo(expected);

        expected = new HashMap<>();
        assertThat(assertions.function("map_topk", "map(array[varchar 'a',varchar 'b',varchar 'c',varchar 'd'], array[double '1.0', double '4.0', double '3.0', double '2.0'])", "0"))
                .hasType(mapType(VarcharType.VARCHAR, DoubleType.DOUBLE))
                .isEqualTo(expected);

        assertThat(assertions.function("map_topk", "map(array[varchar 'a',varchar 'b',varchar 'c',varchar 'd'], array[double '1.0', double '4.0', double '3.0', double '2.0'])", "-1"))
                .hasType(mapType(VarcharType.VARCHAR, DoubleType.DOUBLE))
                .isEqualTo(expected);

        assertions.query("select map_topk(null, 1) is null")
                .assertThat()
                .containsAll("select true");
    }

    @org.testng.annotations.Test
    public void mapTopKBigint()
    {
        HashMap<String, Long> expected = new HashMap<>();
        expected.put("c", (Long) 3L);
        expected.put("b", (Long) 4L);
        assertThat(assertions.function("map_topk", "map(array[varchar 'a',varchar 'b',varchar 'c',varchar 'd'], array[bigint '1', bigint '4', bigint '3', bigint '2'])", "2"))
                .hasType(mapType(VarcharType.VARCHAR, BigintType.BIGINT))
                .isEqualTo(expected);

        expected = new HashMap<>();
        assertThat(assertions.function("map_topk", "map(array[varchar 'a',varchar 'b',varchar 'c',varchar 'd'], array[bigint '1', bigint '4', bigint '3', bigint '2'])", "0"))
                .hasType(mapType(VarcharType.VARCHAR, BigintType.BIGINT))
                .isEqualTo(expected);

        assertThat(assertions.function("map_topk", "map(array[varchar 'a',varchar 'b',varchar 'c',varchar 'd'], array[bigint '1', bigint '4', bigint '3', bigint '2'])", "-1"))
                .hasType(mapType(VarcharType.VARCHAR, BigintType.BIGINT))
                .isEqualTo(expected);

        assertions.query("select map_topk(null, 1) is null")
                .assertThat()
                .containsAll("select true");
    }
}
