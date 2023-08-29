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

import com.google.common.base.Strings;
import io.airlift.slice.Slices;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.VarcharType;
import io.trino.sql.query.QueryAssertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import static org.testng.Assert.assertEquals;

public class TestVideoFunctions
{
    private static final int SEGMENT_COUNT = 100;
    private static final String SEGMENT_COUNT_STR = String.valueOf(SEGMENT_COUNT);
    private static final int VIDEO_LENGTH_5_MINUTES_MS = 5 * 60 * 60 * 1000;
    private static final String VIDEO_LENGTH_5_MINUTES_MS_STR = String.valueOf(VIDEO_LENGTH_5_MINUTES_MS);

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

    @org.testng.annotations.Test
    public void testViewedSegment1()
    {
        String exp = Strings.repeat("f", 24) + "3f";
        assertions.function("viewed_segments", SEGMENT_COUNT_STR, VIDEO_LENGTH_5_MINUTES_MS_STR, "0", VIDEO_LENGTH_5_MINUTES_MS_STR)
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo(exp);
    }

    @org.testng.annotations.Test
    public void testViewedSegment2()
    {
        String exp = "fe" + Strings.repeat("f", 22) + "2f";
        assertions.function("viewed_segments", SEGMENT_COUNT_STR, VIDEO_LENGTH_5_MINUTES_MS_STR, "180000", String.valueOf(VIDEO_LENGTH_5_MINUTES_MS - 1))
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo(exp);

        exp = "ff" + Strings.repeat("f", 22) + "2f";
        assertions.function("viewed_segments", SEGMENT_COUNT_STR, VIDEO_LENGTH_5_MINUTES_MS_STR, "179999", String.valueOf(VIDEO_LENGTH_5_MINUTES_MS - 1))
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo(exp);

        exp = "ff" + Strings.repeat("f", 22) + "3f";
        assertions.function("viewed_segments", SEGMENT_COUNT_STR, VIDEO_LENGTH_5_MINUTES_MS_STR, "179999", String.valueOf(VIDEO_LENGTH_5_MINUTES_MS))
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo(exp);
    }

    @org.testng.annotations.Test
    public void testViewedSegment3()
    {
        String exp = Strings.repeat("f", 24) + "2f";
        assertions.function("viewed_segments", SEGMENT_COUNT_STR, VIDEO_LENGTH_5_MINUTES_MS_STR, "179999", String.valueOf(VIDEO_LENGTH_5_MINUTES_MS - 1))
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo(exp);
    }

    @org.testng.annotations.Test
    public void testViewedSegment4()
    {
        String exp = "ff0f";
        assertions.function("viewed_segments", "10", VIDEO_LENGTH_5_MINUTES_MS_STR, "0", VIDEO_LENGTH_5_MINUTES_MS_STR)
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo(exp);

        exp = "0008";
        assertions.function("viewed_segments", "10", VIDEO_LENGTH_5_MINUTES_MS_STR, "0", "0")
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo(exp);

        exp = Strings.repeat("ff", 125) + "03";
        assertions.function("viewed_segments", "1000", VIDEO_LENGTH_5_MINUTES_MS_STR, "0", VIDEO_LENGTH_5_MINUTES_MS_STR)
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo(exp);

        exp = Strings.repeat("00", 125) + "02";
        assertions.function("viewed_segments", "1000", VIDEO_LENGTH_5_MINUTES_MS_STR, "0", "0")
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo(exp);
    }

    @org.testng.annotations.Test
    public void testViewedSegment5()
    {
        final String videoLength = "1000";
        String exp = "04" + Strings.repeat("0", 22) + "20";
        assertions.function("viewed_segments", SEGMENT_COUNT_STR, videoLength, "20", "21")
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo(exp);

        exp = "06" + Strings.repeat("0", 22) + "20";
        assertions.function("viewed_segments", SEGMENT_COUNT_STR, videoLength, "10", "21")
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo(exp);

        exp = "0c" + Strings.repeat("0", 22) + "20";
        assertions.function("viewed_segments", SEGMENT_COUNT_STR, videoLength, "20", "31")
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo(exp);

        exp = "03" + Strings.repeat("0", 22) + "20";
        assertions.function("viewed_segments", SEGMENT_COUNT_STR, videoLength, "9", "10")
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo(exp);

        exp = "01" + Strings.repeat("0", 22) + "20";
        assertions.function("viewed_segments", SEGMENT_COUNT_STR, videoLength, "8", "9")
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo(exp);

        exp = "01" + Strings.repeat("0", 22) + "20";
        assertions.function("viewed_segments", SEGMENT_COUNT_STR, videoLength, "0", "9")
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo(exp);
    }

    @org.testng.annotations.Test
    public void testViewedSegment6()
    {
        assertEquals(Slices.utf8Slice("04"), VideoFunctions.empty(1));
        assertEquals(Slices.utf8Slice("08"), VideoFunctions.empty(2));
        assertEquals(Slices.utf8Slice("10"), VideoFunctions.empty(3));
        assertEquals(Slices.utf8Slice("20"), VideoFunctions.empty(4));
        assertEquals(Slices.utf8Slice("40"), VideoFunctions.empty(5));
        assertEquals(Slices.utf8Slice("80"), VideoFunctions.empty(6));
        assertEquals(Slices.utf8Slice("0001"), VideoFunctions.empty(7));
        assertEquals(Slices.utf8Slice("0002"), VideoFunctions.empty(8));
        assertEquals(Slices.utf8Slice("0004"), VideoFunctions.empty(9));
        assertEquals(Slices.utf8Slice("0008"), VideoFunctions.empty(10));
        assertEquals(Slices.utf8Slice("0010"), VideoFunctions.empty(11));
        assertEquals(Slices.utf8Slice("0020"), VideoFunctions.empty(12));
        assertEquals(Slices.utf8Slice("0040"), VideoFunctions.empty(13));
        assertEquals(Slices.utf8Slice("0080"), VideoFunctions.empty(14));
        assertEquals(Slices.utf8Slice("000001"), VideoFunctions.empty(15));
        assertEquals(Slices.utf8Slice("000002"), VideoFunctions.empty(16));
    }

    @org.testng.annotations.Test
    public void testViewedSegment7()
    {
        assertions.function("viewed_segments", "100", "5", "0", "5")
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo("ffffffffffffffffffffffff3f");

        assertions.function("viewed_segments", "100", "10", "1", "1")
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo("00000000000000000000000020");

        assertions.function("viewed_segments", "100", "0", "1", "1")
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo("00000000000000000000000020");

        assertions.function("viewed_segments", "100", "0", "0", "1")
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo("00000000000000000000000020");

        assertions.function("viewed_segments", "100", "1", "0", "2")
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo("ffffffffffffffffffffffff3f");

        assertions.function("viewed_segments", "100", "0", "0", "0")
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo("00000000000000000000000020");
    }
}
