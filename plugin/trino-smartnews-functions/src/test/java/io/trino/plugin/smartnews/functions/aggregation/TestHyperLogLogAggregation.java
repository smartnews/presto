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

public class TestHyperLogLogAggregation
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
    public void testPfaggrNull()
    {
        assertThat(assertions.query("SELECT pfcount(cast(pfaggr(k) as varbinary)) FROM (VALUES (cast(null as varchar))) t(k)"))
                .matches("SELECT cast(null as bigint)");
    }

    @org.testng.annotations.Test
    public void testPfmergeNull()
    {
        assertThat(assertions.query("SELECT pfmerge(k) FROM (VALUES (cast(null as varbinary))) t(k)"))
                .matches("SELECT cast(null as varbinary)");
    }

    @org.testng.annotations.Test
    public void testPfaggrString()
    {
        assertThat(assertions.query("SELECT pfcount(cast(pfaggr(k) as varbinary)) FROM (VALUES ('1'), ('1'), ('2')) t(k)"))
                .matches("SELECT BIGINT '2'");
    }

    @org.testng.annotations.Test
    public void testPfaggrBigint()
    {
        assertThat(assertions.query("SELECT pfcount(cast(pfaggr(k) as varbinary)) FROM (VALUES (1), (1), (2)) t(k)"))
                .matches("SELECT BIGINT '2'");
    }

    @org.testng.annotations.Test
    public void testPfaggrDouble()
    {
        assertThat(assertions.query("SELECT pfcount(cast(pfaggr(k) as varbinary)) FROM (VALUES (1.2), (1.2), (1.3)) t(k)"))
                .matches("SELECT BIGINT '2'");
    }

    @org.testng.annotations.Test
    public void testPfmerge()
    {
        assertThat(assertions.query("select pfcount(pfmerge(cast(v as varbinary)))\n" +
                "from (\n" +
                "    select k, pfaggr(v) as v\n" +
                "    from (\n" +
                "        values \n" +
                "        ('a', '1')\n" +
                "        ,('a', '2')\n" +
                "        ,('a', '3')\n" +
                "        ,('b', '3')\n" +
                "        ,('b', '4')\n" +
                "    ) t(k,v)\n" +
                "    group by k\n" +
                ")"))
                .matches("SELECT BIGINT '4'");
    }
}
