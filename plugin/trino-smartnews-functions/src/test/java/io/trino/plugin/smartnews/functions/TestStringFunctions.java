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

import io.trino.spi.type.IntegerType;
import io.trino.sql.query.QueryAssertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public class TestStringFunctions
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
        assertions.function("compare_version", "null", "null")
                .assertThat()
                .isNull(IntegerType.INTEGER);

        assertions.function("compare_version", "null", "'1.2'")
                .assertThat()
                .isNull(IntegerType.INTEGER);

        assertions.function("compare_version", "'1'", "null")
                .assertThat()
                .isNull(IntegerType.INTEGER);

        // same length, equal
        assertions.function("compare_version", "'1.2.34'", "'1.2.34'")
                .assertThat()
                .hasType(IntegerType.INTEGER)
                .isEqualTo(0);

        // same length, less than
        assertions.function("compare_version", "'1.2.34'", "'1.2.35'")
                .assertThat()
                .hasType(IntegerType.INTEGER)
                .isEqualTo(-1);

        // same length, greater than
        assertions.function("compare_version", "'1.3.34'", "'1.2.35'")
                .assertThat()
                .hasType(IntegerType.INTEGER)
                .isEqualTo(1);

        // different length, less than
        assertions.function("compare_version", "'1.2.34'", "'1.2.34.1'")
                .assertThat()
                .hasType(IntegerType.INTEGER)
                .isEqualTo(-1);

        // different length, greater than
        assertions.function("compare_version", "'1.3.34.1'", "'1.2.34'")
                .assertThat()
                .hasType(IntegerType.INTEGER)
                .isEqualTo(1);

        // with characters
        assertions.function("compare_version", "'1.2.34.1'", "'1.2.34.1-fix'")
                .assertThat()
                .hasType(IntegerType.INTEGER)
                .isEqualTo(0);

        assertions.function("compare_version", "'1.2.34.1-abc'", "'1.2.34.1-fix'")
                .assertThat()
                .hasType(IntegerType.INTEGER)
                .isEqualTo(0);

        // illegal char
        assertions.function("compare_version", "'1.2.34.1'", "'1.2.34.1fix'")
                .assertThat()
                .isNull(IntegerType.INTEGER);

        assertions.function("compare_version", "'1.2.34.a'", "'1.2.34.1'")
                .assertThat()
                .isNull(IntegerType.INTEGER);
    }
}
