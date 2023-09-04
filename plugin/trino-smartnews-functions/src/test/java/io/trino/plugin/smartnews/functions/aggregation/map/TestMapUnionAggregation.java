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
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.VarcharType;
import io.trino.sql.query.QueryAssertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.List;

import static io.trino.util.StructuralTestUtil.mapType;

public class TestMapUnionAggregation
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
    public void testUnionSum()
    {
        assertions.query("SELECT map_union_sum(m) FROM (VALUES map(array[varchar 'a', varchar 'b'], array[bigint '1', bigint '2']), map(array[varchar 'a', varchar 'c'], array[bigint '1', bigint '3']), null) t(m)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, BigintType.BIGINT)))
                .containsAll("select map(array[varchar 'a', varchar 'b', varchar 'c'], array[bigint '2', bigint '2', bigint '3'])");

        assertions.query("SELECT map_union_sum(m) FROM (VALUES map(array[varchar 'c', varchar 'b'], array[bigint '3', bigint '2']), map(array[varchar 'a', varchar 'c'], array[bigint '1', null]), null) t(m)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, BigintType.BIGINT)))
                .containsAll("select map(array[varchar 'a', varchar 'b', varchar 'c'], array[bigint '1', bigint '2', bigint '3'])");

        // bigint key
        assertions.query("SELECT map_union_sum(m) FROM (VALUES map(array[bigint '1', bigint '2'], array[bigint '1', bigint '3']), map(array[bigint '1', bigint '3'], array[bigint '1', null]), null) t(m)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(BigintType.BIGINT, BigintType.BIGINT)))
                .containsAll("select map(array[bigint '1', bigint '2', bigint '3'], array[bigint '2', bigint '3', null])");

        // double value
        assertions.query("SELECT map_union_sum(m) FROM (VALUES map(array[varchar 'a', varchar 'b'], array[double '1.1', double '2.2']), map(array[varchar 'a', varchar 'c'], array[double '3.3', double '4.4']), null) t(m)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, DoubleType.DOUBLE)))
                .containsAll("select map(array[varchar 'a', varchar 'b', varchar 'c'], array[double '4.4', double '2.2', double '4.4'])");

        // NaN
        assertions.query("SELECT map_union_sum(m) FROM (VALUES map(array[varchar 'a', varchar 'b'], array[double '1.1', double '2.2']), map(array[varchar 'a', varchar 'c'], array[cast(NaN() as double), double '4.4']), null) t(m)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, DoubleType.DOUBLE)))
                .containsAll("select map(array[varchar 'a', varchar 'b', varchar 'c'], array[cast(NaN() as double), double '2.2', double '4.4'])");

        // real value
        assertions.query("SELECT map_union_sum(m) FROM (VALUES map(array[varchar 'a', varchar 'b'], array[real '1.1', real '2.2']), map(array[varchar 'a', varchar 'c'], array[real '3.3', real '4.4']), null) t(m)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, RealType.REAL)))
                .containsAll("select map(array[varchar 'a', varchar 'b', varchar 'c'], array[real '4.4', real '2.2', real '4.4'])");

        // integer value
        assertions.query("SELECT map_union_sum(m) FROM (VALUES map(array[varchar 'a', varchar 'b'], array[integer '1', integer '2']), map(array[varchar 'a', varchar 'c'], array[integer '3', integer '4']), null) t(m)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, IntegerType.INTEGER)))
                .containsAll("select map(array[varchar 'a', varchar 'b', varchar 'c'], array[integer '4', integer '2', integer '4'])");

        // boolean value
        assertions.query("SELECT map_union_sum(m) FROM (VALUES map(array[varchar 'a', varchar 'b'], array[false, true]), map(array[varchar 'a', varchar 'c'], array[true, false]), null) t(m)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, BooleanType.BOOLEAN)))
                .containsAll("select map(array[varchar 'a', varchar 'b', varchar 'c'], array[true, true, false])");
    }

    @org.testng.annotations.Test
    public void testUnionMax()
    {
        assertions.query("SELECT map_union_max(m) FROM (VALUES map(array[varchar 'a', varchar 'b'], array[bigint '5', bigint '2']), map(array[varchar 'a', varchar 'c'], array[bigint '1', bigint '3']), null) t(m)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, BigintType.BIGINT)))
                .containsAll("select map(array[varchar 'a', varchar 'b', varchar 'c'], array[bigint '5', bigint '2', bigint '3'])");

        assertions.query("SELECT map_union_max(m) FROM (VALUES map(array[varchar 'c', varchar 'b'], array[bigint '3', bigint '2']), map(array[varchar 'a', varchar 'c'], array[bigint '1', null]), null) t(m)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, BigintType.BIGINT)))
                .containsAll("select map(array[varchar 'a', varchar 'b', varchar 'c'], array[bigint '1', bigint '2', bigint '3'])");

        // bigint key
        assertions.query("SELECT map_union_max(m) FROM (VALUES map(array[bigint '1', bigint '2'], array[bigint '5', bigint '3']), map(array[bigint '1', bigint '3'], array[bigint '1', null]), null) t(m)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(BigintType.BIGINT, BigintType.BIGINT)))
                .containsAll("select map(array[bigint '1', bigint '2', bigint '3'], array[bigint '5', bigint '3', null])");

        // double value
        assertions.query("SELECT map_union_max(m) FROM (VALUES map(array[varchar 'a', varchar 'b'], array[double '1.1', double '2.2']), map(array[varchar 'a', varchar 'c'], array[double '3.3', double '4.4']), null) t(m)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, DoubleType.DOUBLE)))
                .containsAll("select map(array[varchar 'a', varchar 'b', varchar 'c'], array[double '3.3', double '2.2', double '4.4'])");

        // NaN
        assertions.query("SELECT map_union_max(m) FROM (VALUES map(array[varchar 'a', varchar 'b'], array[double '1.1', double '2.2']), map(array[varchar 'a', varchar 'c'], array[cast(NaN() as double), double '4.4']), null) t(m)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, DoubleType.DOUBLE)))
                .containsAll("select map(array[varchar 'a', varchar 'b', varchar 'c'], array[double '1.1', double '2.2', double '4.4'])");

        // real value
        assertions.query("SELECT map_union_max(m) FROM (VALUES map(array[varchar 'a', varchar 'b'], array[real '1.1', real '2.2']), map(array[varchar 'a', varchar 'c'], array[real '3.3', real '4.4']), null) t(m)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, RealType.REAL)))
                .containsAll("select map(array[varchar 'a', varchar 'b', varchar 'c'], array[real '3.3', real '2.2', real '4.4'])");

        // integer value
        assertions.query("SELECT map_union_max(m) FROM (VALUES map(array[varchar 'a', varchar 'b'], array[integer '1', integer '2']), map(array[varchar 'a', varchar 'c'], array[integer '3', integer '4']), null) t(m)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, IntegerType.INTEGER)))
                .containsAll("select map(array[varchar 'a', varchar 'b', varchar 'c'], array[integer '3', integer '2', integer '4'])");

        // boolean value
        assertions.query("SELECT map_union_max(m) FROM (VALUES map(array[varchar 'a', varchar 'b'], array[false, true]), map(array[varchar 'a', varchar 'c'], array[false, false]), null) t(m)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, BooleanType.BOOLEAN)))
                .containsAll("select map(array[varchar 'a', varchar 'b', varchar 'c'], array[false, true, false])");
    }

    @org.testng.annotations.Test
    public void testUnionMin()
    {
        assertions.query("SELECT map_union_min(m) FROM (VALUES map(array[varchar 'a', varchar 'b'], array[bigint '5', bigint '2']), map(array[varchar 'a', varchar 'c'], array[bigint '1', bigint '3']), null) t(m)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, BigintType.BIGINT)))
                .containsAll("select map(array[varchar 'a', varchar 'b', varchar 'c'], array[bigint '1', bigint '2', bigint '3'])");

        assertions.query("SELECT map_union_min(m) FROM (VALUES map(array[varchar 'c', varchar 'b'], array[bigint '3', bigint '2']), map(array[varchar 'a', varchar 'c'], array[bigint '1', null]), null) t(m)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, BigintType.BIGINT)))
                .containsAll("select map(array[varchar 'a', varchar 'b', varchar 'c'], array[bigint '1', bigint '2', bigint '3'])");

        // bigint key
        assertions.query("SELECT map_union_min(m) FROM (VALUES map(array[bigint '1', bigint '2'], array[bigint '5', bigint '3']), map(array[bigint '1', bigint '3'], array[bigint '1', null]), null) t(m)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(BigintType.BIGINT, BigintType.BIGINT)))
                .containsAll("select map(array[bigint '1', bigint '2', bigint '3'], array[bigint '1', bigint '3', null])");

        // double value
        assertions.query("SELECT map_union_min(m) FROM (VALUES map(array[varchar 'a', varchar 'b'], array[double '1.1', double '2.2']), map(array[varchar 'a', varchar 'c'], array[double '3.3', double '4.4']), null) t(m)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, DoubleType.DOUBLE)))
                .containsAll("select map(array[varchar 'a', varchar 'b', varchar 'c'], array[double '1.1', double '2.2', double '4.4'])");

        // NaN
        assertions.query("SELECT map_union_min(m) FROM (VALUES map(array[varchar 'a', varchar 'b'], array[double '1.1', double '2.2']), map(array[varchar 'a', varchar 'c'], array[cast(NaN() as double), double '4.4']), null) t(m)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, DoubleType.DOUBLE)))
                .containsAll("select map(array[varchar 'a', varchar 'b', varchar 'c'], array[cast(NaN() as double), double '2.2', double '4.4'])");

        // real value
        assertions.query("SELECT map_union_min(m) FROM (VALUES map(array[varchar 'a', varchar 'b'], array[real '1.1', real '2.2']), map(array[varchar 'a', varchar 'c'], array[real '3.3', real '4.4']), null) t(m)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, RealType.REAL)))
                .containsAll("select map(array[varchar 'a', varchar 'b', varchar 'c'], array[real '1.1', real '2.2', real '4.4'])");

        // integer value
        assertions.query("SELECT map_union_min(m) FROM (VALUES map(array[varchar 'a', varchar 'b'], array[integer '1', integer '2']), map(array[varchar 'a', varchar 'c'], array[integer '3', integer '4']), null) t(m)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, IntegerType.INTEGER)))
                .containsAll("select map(array[varchar 'a', varchar 'b', varchar 'c'], array[integer '1', integer '2', integer '4'])");

        // boolean value
        assertions.query("SELECT map_union_min(m) FROM (VALUES map(array[varchar 'a', varchar 'b'], array[false, true]), map(array[varchar 'a', varchar 'c'], array[true, false]), null) t(m)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, BooleanType.BOOLEAN)))
                .containsAll("select map(array[varchar 'a', varchar 'b', varchar 'c'], array[false, true, false])");
    }
}
