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
package io.trino.plugin.smartnews.functions.aggregation.map;

import io.trino.operator.scalar.AbstractTestFunctions;
import io.trino.plugin.smartnews.functions.SmartnewsFunctionsPlugin;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.VarcharType;
import io.trino.sql.query.QueryAssertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.List;

import static io.trino.util.StructuralTestUtil.mapType;

public class TestMapTopkAggregation
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
    public void testTopkAggVarcharBigint()
    {
        assertions.query("SELECT map_agg_topk(integer '3', cast(k as varchar), cast(v as bigint)) FROM (VALUES ('a', 1),('b', 2),('a', 3),('c', 4)) t(k,v)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, BigintType.BIGINT)))
                .containsAll("select map(array[varchar 'a', varchar 'b', varchar 'c'], array[bigint '3', bigint '2', bigint '4'])");

        // k = 0
        assertions.query("SELECT cardinality(map_agg_topk(integer '0', cast(k as varchar), cast(v as bigint))) as s FROM (VALUES ('a', 1),('b', 2),('a', 3),('c', 4)) t(k,v)")
                .assertThat()
                .hasOutputTypes(List.of(BigintType.BIGINT))
                .containsAll("select bigint '0' as s");

        // k < 0
        assertions.query("SELECT cardinality(map_agg_topk(integer '-1', cast(k as varchar), cast(v as bigint))) as s FROM (VALUES ('a', 1),('b', 2),('a', 3),('c', 4)) t(k,v)")
                .assertThat()
                .hasOutputTypes(List.of(BigintType.BIGINT))
                .containsAll("select bigint '0' as s");

        // null key and value
        assertions.query("SELECT map_agg_topk(integer '3', cast(k as varchar), cast(v as bigint)) FROM (VALUES ('a', 1),('b', 2),('a', 3),('c', 4),(null, null)) t(k,v)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, BigintType.BIGINT)))
                .containsAll("select map(array[varchar 'a', varchar 'b', varchar 'c'], array[bigint '3', bigint '2', bigint '4'])");

        // value is null
        assertions.query("SELECT map_agg_topk(integer '1', cast(k as varchar), cast(v as bigint)) FROM (VALUES ('a', null),('b', 3)) t(k,v)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, BigintType.BIGINT)))
                .containsAll("select map(array[varchar 'b'], array[bigint '3'])");

        // key is null
        assertions.query("SELECT map_agg_topk(integer '1', cast(k as varchar), cast(v as bigint)) FROM (VALUES (null, 1),('b', 3)) t(k,v)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, BigintType.BIGINT)))
                .containsAll("select map(array[varchar 'b'], array[bigint '3'])");
    }

    @org.testng.annotations.Test
    public void testTopkAggVarcharDouble()
    {
        assertions.query("SELECT map_agg_topk(integer '3', cast(k as varchar), cast(v as double)) FROM (VALUES ('a', 1.1),('b', 2.2),('a', 3.3),('c', 4.4)) t(k,v)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, DoubleType.DOUBLE)))
                .containsAll("select map(array[varchar 'a', varchar 'b', varchar 'c'], array[double '3.3', double '2.2', double '4.4'])");

        // k = 0
        assertions.query("SELECT cardinality(map_agg_topk(integer '0', cast(k as varchar), cast(v as double))) as s FROM (VALUES ('a', 1.1),('b', 2.2),('a', 3.3),('c', 4.4)) t(k,v)")
                .assertThat()
                .hasOutputTypes(List.of(BigintType.BIGINT))
                .containsAll("select bigint '0' as s");

        // k < 0
        assertions.query("SELECT cardinality(map_agg_topk(integer '-1', cast(k as varchar), cast(v as double))) as s FROM (VALUES ('a', 1.1),('b', 2.2),('a', 3.3),('c', 4.4)) t(k,v)")
                .assertThat()
                .hasOutputTypes(List.of(BigintType.BIGINT))
                .containsAll("select bigint '0' as s");

        // null key and value
        assertions.query("SELECT map_agg_topk(integer '3', cast(k as varchar), cast(v as double)) FROM (VALUES ('a', 3.3),('b', 2.2),('a', 1.1),('c', 4.4),(null, null)) t(k,v)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, DoubleType.DOUBLE)))
                .containsAll("select map(array[varchar 'a', varchar 'b', varchar 'c'], array[double '3.3', double '2.2', double '4.4'])");

        // value is null
        assertions.query("SELECT map_agg_topk(integer '1', cast(k as varchar), cast(v as double)) FROM (VALUES ('a', null),('b', 3.3)) t(k,v)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, DoubleType.DOUBLE)))
                .containsAll("select map(array[varchar 'b'], array[double '3.3'])");

        // key is null
        assertions.query("SELECT map_agg_topk(integer '1', cast(k as varchar), cast(v as double)) FROM (VALUES (null, 1),('b', 3.3)) t(k,v)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, DoubleType.DOUBLE)))
                .containsAll("select map(array[varchar 'b'], array[double '3.3'])");
    }

    @org.testng.annotations.Test
    public void testTopkAggBigintBigint()
    {
        assertions.query("SELECT map_agg_topk(integer '3', cast(k as bigint), cast(v as bigint)) FROM (VALUES (1,100),(2,200),(3,300),(1,400)) t(k,v)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(BigintType.BIGINT, BigintType.BIGINT)))
                .containsAll("select map(array[bigint '1', bigint '2', bigint '3'], array[bigint '400', bigint '200', bigint '300'])");

        // k = 0
        assertions.query("SELECT cardinality(map_agg_topk(integer '0', cast(k as bigint), cast(v as bigint))) as s FROM (VALUES (1,100),(2,200),(3,300),(4,400)) t(k,v)")
                .assertThat()
                .hasOutputTypes(List.of(BigintType.BIGINT))
                .containsAll("select bigint '0' as s");

        // k < 0
        assertions.query("SELECT cardinality(map_agg_topk(integer '-1', cast(k as bigint), cast(v as bigint))) as s FROM (VALUES (1,100),(2,200),(3,300),(4,400)) t(k,v)")
                .assertThat()
                .hasOutputTypes(List.of(BigintType.BIGINT))
                .containsAll("select bigint '0' as s");

        // null key and value
        assertions.query("SELECT map_agg_topk(integer '3', cast(k as bigint), cast(v as bigint)) FROM (VALUES (1,100),(2,200),(1,300),(3,400),(null, null)) t(k,v)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(BigintType.BIGINT, BigintType.BIGINT)))
                .containsAll("select map(array[bigint '1', bigint '2', bigint '3'], array[bigint '300', bigint '200', bigint '400'])");

        // value is null
        assertions.query("SELECT map_agg_topk(integer '1', cast(k as bigint), cast(v as bigint)) FROM (VALUES (1, null),(2, 200)) t(k,v)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(BigintType.BIGINT, BigintType.BIGINT)))
                .containsAll("select map(array[bigint '2'], array[bigint '200'])");

        // key is null
        assertions.query("SELECT map_agg_topk(integer '1', cast(k as bigint), cast(v as bigint)) FROM (VALUES (null, 1),(2, 200)) t(k,v)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(BigintType.BIGINT, BigintType.BIGINT)))
                .containsAll("select map(array[bigint '2'], array[bigint '200'])");
    }
}
