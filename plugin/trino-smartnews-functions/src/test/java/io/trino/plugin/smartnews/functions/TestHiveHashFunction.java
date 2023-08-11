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
import io.trino.sql.query.QueryAssertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public class TestHiveHashFunction
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
        assertions.function("hash", "null")
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(0L);

        assertions.function("hash", "1")
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(1L);

        assertions.function("hash", "true")
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(1L);

        assertions.function("hash", "false")
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(0L);

        assertions.function("hash", "cast(3 as tinyint)")
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(3L);

        assertions.function("hash", "cast(257 as smallint)")
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(257L);

        assertions.function("hash", "cast(1234567 as integer)")
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(1234567L);

        assertions.function("hash", "cast(12345678910123 as bigint)")
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(1942903185L);

        assertions.function("hash", "cast(3.1415926 as real)")
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(1078530010L);

        assertions.function("hash", "cast(3.14159265358972 as double)")
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(340594056L);

        assertions.function("hash", "'Hi_Smartnews'")
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(74449502L);

        assertions.function("hash", "array['hi', cast(null as varchar), 'smartnews']")
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(-1440095011L);

        assertions.function("hash", "array[integer '2023', cast(null as integer), integer '2024']")
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(1946127L);

        // array of float
        assertions.function("hash", "array[REAL '3.14159', REAL '-3.14']")
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(-1994268685L);

        assertions.function("hash", "map(array['a', 'b'], array[1, 2])")
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(192L);

        // map of array
        assertions.function("hash", "map(array['abcedfg'], array[array[1980, 20232023]])")
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(-1188281831L);

        // array of 1 map with 1 KV
        assertions.function("hash", "array[map(array['kjkj'], array[integer '3'])]")
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(3292925L);

        // array of 1 map with 2 KV
        assertions.function("hash", "array[map(array['kjkj', 'Ji_jkj'], array[integer '3', integer '9990'])]")
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(-2073204116L);

        // array of 2 map, each with 1 KV
        assertions.function("hash", "array[map(array['kjkj'], array[integer '3']), map(array['Ji_jkj'], array[integer '9990'])]")
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(-1974416366L);

        // row
        assertions.function("hash", "row('hello', integer '20230714', true, bigint '1689305619', array['ni', 'hao'])")
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(-874017887L);

        // nested row
        assertions.function("hash", "row('row in row', row('Zhangsan', integer '2345'))")
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(-1306741168L);

        // decimal
        assertions.function("hash", "DECIMAL '10.31784'")
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(31985309L);

        // date
        assertions.function("hash", "date('2023-07-13')") // hello
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(19551L);

        // timestamp 1
        assertions.function("hash", "timestamp '2023-07-13 23:59:59'")
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(-651418625L);

        // timestamp 2
        assertions.function("hash", "timestamp '2020-06-10 15:55:23.123'")
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(-789801894L);

        // timestamp 3
        assertions.function("hash", "timestamp '2020-06-10 15:55:23.123456'") // hello
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(-790394214L);

        // timestamp 4
        assertions.function("hash", "timestamp '2020-06-10 15:55:23.123456789'")
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(-790393457L);

        // binary
        assertions.function("hash", "cast('hello' as varbinary)")
                .assertThat()
                .hasType(BigintType.BIGINT)
                .isEqualTo(127791473L);
    }
}
