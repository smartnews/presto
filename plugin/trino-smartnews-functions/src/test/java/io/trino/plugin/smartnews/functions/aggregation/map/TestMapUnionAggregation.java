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
    public void test01()
    {
        assertions.query("SELECT union_map(m) FROM (VALUES map(array[varchar 'a', varchar 'b'], array[bigint '1', bigint '2']), map(array[varchar 'a', varchar 'c'], array[bigint '1', bigint '3']), null) t(m)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, BigintType.BIGINT)))
                .containsAll("select map(array[varchar 'a', varchar 'b', varchar 'c'], array[bigint '2', bigint '2', bigint '3'])");
    }

    @org.testng.annotations.Test
    public void testNull()
    {
        assertions.query("SELECT union_map(m) FROM (VALUES null) t(m)")
                .assertThat()
                .hasOutputTypes(List.of(mapType(VarcharType.VARCHAR, BigintType.BIGINT)))
                .containsAll("select cast(null as map(varchar, bigint))");
    }
}
