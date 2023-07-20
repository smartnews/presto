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

import io.airlift.slice.Slice;
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
}
