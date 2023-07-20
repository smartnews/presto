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

import io.trino.spi.type.BooleanType;
import io.trino.sql.query.QueryAssertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public class TestVideoFunctions
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
    public void testVideoViewed()
    {
        String segment = "";
        segment = "f".repeat(24) + "3f";
        assertions.function("video_viewed", "'" + segment + "'", "0")
                .assertThat()
                .hasType(BooleanType.BOOLEAN)
                .isEqualTo(true);

        segment = "f".repeat(24) + "3f";
        assertions.function("video_viewed", "'" + segment + "'", "1")
                .assertThat()
                .hasType(BooleanType.BOOLEAN)
                .isEqualTo(true);

        segment = "0".repeat(24) + "20";
        assertions.function("video_viewed", "'" + segment + "'", "0")
                .assertThat()
                .hasType(BooleanType.BOOLEAN)
                .isEqualTo(false);

        segment = "1".repeat(24) + "20";
        assertions.function("video_viewed", "'" + segment + "'", "0")
                .assertThat()
                .hasType(BooleanType.BOOLEAN)
                .isEqualTo(true);

        segment = "0".repeat(24) + "20";
        assertions.function("video_viewed", "'" + segment + "'", "0.001")
                .assertThat()
                .hasType(BooleanType.BOOLEAN)
                .isEqualTo(false);

        segment = "1".repeat(24) + "20";
        assertions.function("video_viewed", "'" + segment + "'", "0.001")
                .assertThat()
                .hasType(BooleanType.BOOLEAN)
                .isEqualTo(true);

        segment = "0".repeat(24) + "20";
        assertions.function("video_viewed", "'" + segment + "'", "0.01")
                .assertThat()
                .hasType(BooleanType.BOOLEAN)
                .isEqualTo(false);

        segment = "1".repeat(24) + "20";
        assertions.function("video_viewed", "'" + segment + "'", "0.01")
                .assertThat()
                .hasType(BooleanType.BOOLEAN)
                .isEqualTo(true);

        segment = "0".repeat(24) + "20";
        assertions.function("video_viewed", "'" + segment + "'", "1")
                .assertThat()
                .hasType(BooleanType.BOOLEAN)
                .isEqualTo(false);

        segment = "0".repeat(24) + "30";
        assertions.function("video_viewed", "'" + segment + "'", "1")
                .assertThat()
                .hasType(BooleanType.BOOLEAN)
                .isEqualTo(true);

        segment = "0".repeat(24) + "30";
        assertions.function("video_viewed", "'" + segment + "'", "0")
                .assertThat()
                .hasType(BooleanType.BOOLEAN)
                .isEqualTo(true);

        segment = "0".repeat(24) + "30";
        assertions.function("video_viewed", "'" + segment + "'", "0.01")
                .assertThat()
                .hasType(BooleanType.BOOLEAN)
                .isEqualTo(true);

        segment = "0".repeat(24) + "30";
        assertions.function("video_viewed", "'" + segment + "'", "0.99")
                .assertThat()
                .hasType(BooleanType.BOOLEAN)
                .isEqualTo(true);

        segment = "0".repeat(24) + "2d";
        assertions.function("video_viewed", "'" + segment + "'", "1")
                .assertThat()
                .hasType(BooleanType.BOOLEAN)
                .isEqualTo(false);

        segment = "0".repeat(24) + "2d";
        assertions.function("video_viewed", "'" + segment + "'", "0.99")
                .assertThat()
                .hasType(BooleanType.BOOLEAN)
                .isEqualTo(true);
    }
}
