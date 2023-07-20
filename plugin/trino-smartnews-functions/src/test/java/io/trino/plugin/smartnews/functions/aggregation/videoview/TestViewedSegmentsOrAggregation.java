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
package io.trino.plugin.smartnews.functions.aggregation.videoview;

import io.trino.operator.scalar.AbstractTestFunctions;
import io.trino.plugin.smartnews.functions.SmartnewsFunctionsPlugin;
import io.trino.sql.query.QueryAssertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public class TestViewedSegmentsOrAggregation
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
    public void testViewedSegmentsOr()
    {
        assertions.query("SELECT viewed_segments_or(k) FROM (VALUES '04000000000000000000000020', '04000000000000000000000020') t(k)")
                .assertThat()
                .matches("SELECT cast('04000000000000000000000020' as varchar)");

        assertions.query("SELECT viewed_segments_or(k) FROM (VALUES '04003100000000000000000020', '0108') t(k)")
                .assertThat()
                .matches("SELECT cast('ff033100000000000000000020' as varchar)");

        assertions.query("SELECT viewed_segments_or(k) FROM (VALUES '0108', '04003100000000000000000020', '0108') t(k)")
                .assertThat()
                .matches("SELECT cast('ff033100000000000000000020' as varchar)");

        assertions.query("SELECT viewed_segments_or(k) FROM (VALUES '04000000000000000000000026', '05000000000000000000000020', '1030000000000000000000dc28') t(k)")
                .assertThat()
                .matches("SELECT cast('1530000000000000000000dc2e' as varchar)");
    }
}
