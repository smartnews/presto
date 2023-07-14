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

import io.trino.spi.type.VarcharType;
import io.trino.sql.query.QueryAssertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public class TestJsonFunctions
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
    public void testBasic()
    {
        assertions.function("merge_json", "null", "null")
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo("{}");

        assertions.function("merge_json", "null", "'{}'")
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo("{}");

        assertions.function("merge_json", "'{}'", "null")
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo("{}");

        assertions.function("merge_json", "'{}'", "'{}'")
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo("{}");

        assertions.function("merge_json", "'{\"a\": 1, \"b\": 2}'", "'{}'")
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo("{\"a\":1,\"b\":2}");

        assertions.function("merge_json", "'{\"a\": 1, \"b\": 2}'", "'{\"c\": 3}'")
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo("{\"a\":1,\"b\":2,\"c\":3}");

        assertions.function("merge_json", "'{\"a\": 1}'", "'{\"b\": 2}'", "'{\"c\": 3}'")
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo("{\"a\":1,\"b\":2,\"c\":3}");

        assertions.function("merge_json", "'{\"a\": 1, \"b\": 2, \"c\":[1,2], \"d\":{\"da\":3.14}}'", "'{\"e\": 3}'")
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo("{\"a\":1,\"b\":2,\"c\":[1,2],\"d\":{\"da\":3.14},\"e\":3}");
    }
}
