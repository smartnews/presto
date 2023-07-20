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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.VarcharType;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.sql.SQLException;
import java.util.BitSet;
import java.util.Locale;

@AggregationFunction("viewed_segments_or")
public class ViewedSegmentsOrAggregation
{
    private ViewedSegmentsOrAggregation() {}

    public static BitSet newStateValue()
    {
        return new BitSet();
    }

    @InputFunction
    public static void input(@AggregationState BitSetState state, @SqlType(StandardTypes.VARCHAR) Slice segment)
            throws SQLException
    {
        BitSet currentBitSet = getOrCreateState(state);

        if (segment != null) {
            try {
                final String partialString = segment.toStringUtf8();
                BitSet newBitSet = BitSet.valueOf(Hex.decodeHex(partialString.toCharArray()));
                int newLength = newBitSet.length() - 2;
                int currentLength = currentBitSet.length() - 2;

                if (!currentBitSet.isEmpty()) {
                    if (newLength > currentLength) {
                        int ratio = newLength / currentLength;
                        if (currentLength * ratio != newLength) {
                            throw new SQLException("BitSet length must be integral multiple");
                        }
                        BitSet tmp = new BitSet(newLength + 2);
                        for (int i = 0; i < newLength; i++) {
                            tmp.set(i, currentBitSet.get(i / ratio));
                        }
                        tmp.set(newLength, currentBitSet.get(currentBitSet.length() - 2));
                        tmp.set(newLength + 1, true);
                        currentBitSet.clear();
                        currentBitSet.or(tmp);
                    }
                    else if (newLength < currentLength) {
                        int ratio = currentLength / newLength;
                        if (newLength * ratio != currentLength) {
                            throw new SQLException("BitSet length must be integral multiple");
                        }
                        BitSet tmp = new BitSet(currentLength + 2);
                        for (int i = 0; i < currentLength; i++) {
                            tmp.set(i, newBitSet.get(i / ratio));
                        }
                        tmp.set(currentLength, newBitSet.get(newBitSet.length() - 2));
                        tmp.set(currentLength + 1, true);
                        newBitSet = tmp;
                    }
                }
                currentBitSet.or(newBitSet);
            }
            catch (DecoderException e) {
                throw new SQLException(e);
            }
        }
    }

    private static BitSet getOrCreateState(@AggregationState BitSetState state)
    {
        BitSet m = state.getBitSet();
        if (m == null) {
            m = newStateValue();
            state.setBitSet(m);
        }
        return m;
    }

    @CombineFunction
    public static void combineState(@AggregationState BitSetState state, @AggregationState BitSetState otherState)
    {
        BitSet input = otherState.getBitSet();

        BitSet previous = state.getBitSet();
        if (previous == null) {
            state.setBitSet(input);
        }
        else {
            previous.or(input);
        }
    }

    @OutputFunction(StandardTypes.VARCHAR)
    public static void evaluateFinal(@AggregationState BitSetState state, BlockBuilder out)
    {
        BitSet block = state.getBitSet();
        if (block == null) {
            out.appendNull();
            return;
        }
        StringBuilder hex = new StringBuilder();
        for (int k = 0; k < block.toByteArray().length; k++) {
            String result = String.format("%02X", block.toByteArray()[k]);
            hex.append(result);
        }
        String outtext = hex.toString().toLowerCase(Locale.ROOT);
        VarcharType.VARCHAR.writeSlice(out, Slices.utf8Slice(outtext));
    }
}
