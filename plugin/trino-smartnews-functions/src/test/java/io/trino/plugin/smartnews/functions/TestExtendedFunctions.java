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
import io.trino.spi.type.VarcharType;
import io.trino.sql.query.QueryAssertions;
import io.trino.type.JsonType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import static org.assertj.core.api.Assertions.assertThat;

public class TestExtendedFunctions
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
    public void testFj()
    {
        assertThat(assertions.function("fj", "'{\"a\":\"val\"}'", "'$.a'"))
                .hasType(VarcharType.VARCHAR)
                .isEqualTo("val");
    }

    @org.testng.annotations.Test
    public void testJq()
    {
        assertThat(assertions.function("jq", "'{\"a\":\"val\"}'", "'.a'"))
                .hasType(JsonType.JSON)
                .isEqualTo("[\"val\"]");
    }

    @org.testng.annotations.Test
    public void testHashInto()
    {
        assertThat(assertions.function("hash_into", "'sn'", "1024"))
                .hasType(BigintType.BIGINT)
                .isEqualTo(604L);
    }

    @org.testng.annotations.Test
    public void testRawMHash()
    {
        assertThat(assertions.function("raw_mhash", "1024", "'sn'"))
                .hasType(BigintType.BIGINT)
                .isEqualTo(513317058L);
    }
}
