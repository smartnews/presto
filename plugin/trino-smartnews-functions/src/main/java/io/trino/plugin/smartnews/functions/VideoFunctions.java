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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.apache.commons.codec.binary.Hex;

import java.util.BitSet;

public class VideoFunctions
{
    private VideoFunctions() {}

    @Description("Same as Hive UDF video_viewed")
    @ScalarFunction("video_viewed")
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean videoViewed(@SqlType(StandardTypes.VARCHAR) Slice segment, @SqlType(StandardTypes.DOUBLE) double threshold)
            throws Exception
    {
        try {
            String segmentsString = segment.toStringUtf8();
            BitSet bitSet = BitSet.valueOf(Hex.decodeHex(segmentsString.toCharArray()));
            int segmentsCount = bitSet.length() - 2;
            bitSet = bitSet.get(0, bitSet.length() - 1);

            int thresholdSegment = (int) (segmentsCount * threshold);
            if (thresholdSegment < 0) {
                return true;
            }

            if (thresholdSegment > segmentsCount) {
                thresholdSegment = segmentsCount;
            }

            return !bitSet.get(thresholdSegment, segmentsCount + 1).isEmpty();
        }
        catch (Exception e) {
            return null;
        }
    }

    @Description("Same as Hive UDF viewed_segments")
    @ScalarFunction("viewed_segments")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice viewedSegment(@SqlType(StandardTypes.INTEGER) long segmentCount,
                                      @SqlType(StandardTypes.INTEGER) long videoLength,
                                      @SqlType(StandardTypes.INTEGER) long startTime,
                                      @SqlType(StandardTypes.INTEGER) long endTime)
            throws Exception
    {
        if (startTime > videoLength) {
            startTime = videoLength;
        }
        if (endTime > videoLength) {
            endTime = videoLength;
        }

        if (startTime >= endTime) {
            return empty((int) segmentCount);
        }

        double segmentSize = (videoLength / (double) segmentCount);
        int startSegment = (int) (startTime / segmentSize);
        int endSegment = (int) (endTime / segmentSize);

        if (startSegment < 0) {
            startSegment = 0;
        }
        if (endSegment < 0) {
            return empty((int) segmentCount);
        }

        int paddingLength = (int) (8 - (segmentCount + 2) % 8);
        String binStr = Strings.repeat("0", startSegment) +
                Strings.repeat("1", endSegment - startSegment + 1) +
                Strings.repeat("0", (int) (segmentCount - endSegment)) + '1' + Strings.repeat("0", paddingLength);

        String text = "";
        for (int i = 0; i < binStr.length(); i += 8) {
            String reversed = new StringBuilder(binStr.substring(i, i + 8)).reverse().toString();
            String hex = Integer.toHexString(Integer.parseInt(reversed, 2));
            if (hex.length() == 1) {
                hex = '0' + hex;
            }
            text += hex;
        }
        if (text.length() % 2 != 0) {
            text = text.substring(0, text.length() - 1) + '0' + text.charAt(text.length() - 1);
        }
        return Slices.utf8Slice(text);
    }

    public static Slice empty(int segmentCount)
    {
        int paddingLength = (8 - ((segmentCount + 2) % 8)) % 8;
        int count = (segmentCount + 1) / 8;
        String text = Strings.repeat("00", count);
        String tail = Integer.toHexString(128 >> paddingLength);
        if (tail.length() == 1) {
            tail = '0' + tail;
        }
        return Slices.utf8Slice(text + tail);
    }
}
