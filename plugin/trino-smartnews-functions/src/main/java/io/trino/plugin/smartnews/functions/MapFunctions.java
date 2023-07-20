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

import com.google.common.base.Joiner;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TypeOperators;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class MapFunctions
{
    private MapFunctions()
    {
    }

    @Description("Normalize map's value based on the total sum")
    @ScalarFunction("map_norm")
    @SqlType("map(varchar,double)")
    public static Block normMap(@SqlType("map(varchar,double)") Block mapblock,
                                   @SqlType(StandardTypes.DOUBLE) double precision)
            throws Exception
    {
        double sum = 0.0;
        if (mapblock != null) {
            for (int keyPosition = 0; keyPosition < mapblock.getPositionCount(); keyPosition += 2) {
                Object valueObj = readNativeValue(DOUBLE, mapblock, keyPosition + 1);
                if (valueObj instanceof Double value) {
                    sum += value;
                }
            }
        }
        if (sum == 0.0) {
            return mapblock;
        }

        double roundScale = precision >= 0 ? Math.pow(10, precision) : 0.0;

        MapType mapType = new MapType(VARCHAR, DOUBLE, new TypeOperators());
        BlockBuilder mapBlockBuilder = mapType.createBlockBuilder(null, mapblock.getPositionCount());
        BlockBuilder entryBuilder = mapBlockBuilder.beginBlockEntry();
        if (mapblock != null) {
            for (int keyPosition = 0; keyPosition < mapblock.getPositionCount(); keyPosition += 2) {
                int keylen = mapblock.getSliceLength(keyPosition);
                Object valueObj = readNativeValue(DOUBLE, mapblock, keyPosition + 1);
                if (valueObj instanceof Double value) {
                    VARCHAR.writeSlice(entryBuilder, mapblock.getSlice(keyPosition, 0, keylen));
                    double newValue = truncateDoubleIfNeeded(value / sum, roundScale);
                    DOUBLE.writeDouble(entryBuilder, newValue);
                }
            }
        }
        mapBlockBuilder.closeEntry();

        Block ret = mapType.getObject(mapBlockBuilder.build(), 0);
        return ret;
    }

    @Description("Normalize map's value based on the total sum")
    @ScalarFunction("map_norm")
    @SqlType("map(varchar,double)")
    public static Block normMap(@SqlType("map(varchar,double)") Block mapblock)
            throws Exception
    {
        return normMap(mapblock, -1.0);
    }

    @Description("_FUNC_(text, delimiter1, delimiter2) - "
            + "Creates a map by parsing text. Split text into key-value pairs"
            + " using two delimiters. The first delimiter separates pairs, and the"
            + " second delimiter sperates key and value. If only one parameter is given, default"
            + " delimiters are used: ',' as delimiter1 and ':' as delimiter2.")
    @ScalarFunction("str_to_map")
    @SqlType("map(varchar,varchar)")
    public static Block string2map(@SqlType(StandardTypes.VARCHAR) Slice slice,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice delimiter1,
                                   @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice delimiter2)
            throws Exception
    {
        String str = slice.toStringUtf8();
        String delimiter1Str = delimiter1 == null ? "," : delimiter1.toStringUtf8();
        String delimiter2Str = delimiter2 == null ? ":" : delimiter2.toStringUtf8();
        String[] pairs = str.split(delimiter1Str);
        MapType mapType = new MapType(VARCHAR, VARCHAR, new TypeOperators());
        BlockBuilder mapBlockBuilder = mapType.createBlockBuilder(null, pairs.length);
        BlockBuilder entryBuilder = mapBlockBuilder.beginBlockEntry();
        for (String row : pairs) {
            String[] kv = row.split(delimiter2Str);
            if (kv.length == 2) {
                VARCHAR.writeSlice(entryBuilder, Slices.utf8Slice(kv[0]));
                VARCHAR.writeSlice(entryBuilder, Slices.utf8Slice(kv[1]));
            }
            else if (kv.length == 1) {
                VARCHAR.writeSlice(entryBuilder, Slices.utf8Slice(kv[0]));
                entryBuilder.appendNull();
            }
        }
        mapBlockBuilder.closeEntry();

        Block ret = mapType.getObject(mapBlockBuilder.build(), 0);
        return ret;
    }

    @Description("_FUNC_(text, delimiter1, delimiter2) - "
            + "Creates a map by parsing text. Split text into key-value pairs"
            + " using two delimiters. The first delimiter separates pairs, and the"
            + " second delimiter sperates key and value. If only one parameter is given, default"
            + " delimiters are used: ',' as delimiter1 and ':' as delimiter2.")
    @ScalarFunction("str_to_map")
    @SqlType("map(varchar,varchar)")
    public static Block string2map(@SqlType(StandardTypes.VARCHAR) Slice slice)
            throws Exception
    {
        return string2map(slice, null, null);
    }

    @Description("_FUNC_(fv, bucket_start, idmap_file) - Re-mapping fid in libsvm fv")
    @ScalarFunction("remap_libsvm")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice remapLibsvm(@SqlType(StandardTypes.VARCHAR) Slice slice,
                                    @SqlType(StandardTypes.INTEGER) long bucketStart,
                                    @SqlType(StandardTypes.VARCHAR) Slice idmapFile)
            throws Exception
    {
        String idmapFileStr = idmapFile.toStringUtf8();
        if (!"dummy".equalsIgnoreCase(idmapFileStr)) {
            throw new Exception("3rd argument idmap_file must be 'dummy', otherwise not implemented yet");
        }

        String uf = slice.toStringUtf8();
        TreeMap<Integer, Double> map = new TreeMap<>();

        for (String s : uf.split("[ ]+")) {
            String[] split = s.split(":");
            if (split.length == 2) {
                //Integer fid = Integer.valueOf(split[0]);
                try {
                    Double v = Double.valueOf(split[1]);
                    if (true /*idmapDummy*/) {
                        if (false /*rebucket*/) {
                            map.put((int) bucketStart + Integer.valueOf(split[0]) % 100_00000 /*rebucketSize*/, v);
                        }
                        else {
                            map.put((int) bucketStart + Integer.valueOf(split[0]), v);
                        }
                    }
//                    else {
//                        if (idmap.containsKey(split[0])) {
//                            if (rebucket) {
//                                map.put(bucketStart + idmap.get(split[0]) % rebucketSize, v);
//                            }
//                            else {
//                                map.put(bucketStart + idmap.get(split[0]), v);
//                            }
//                        }
//                        else {
//                            //throw new HiveException("Fid " + fid + " not int idmap");
//                            //LOG.info("Fid " + fid + " not int idmap");
//                        }
//                    }
                }
                catch (NumberFormatException e) {
                }
            }
        }

        List<String> output = new ArrayList<>();
        for (Map.Entry<Integer, Double> entry : map.entrySet()) {
            output.add(String.format("%d:%s", entry.getKey(),
                    new DecimalFormat("#.#####").format(entry.getValue())));
        }

        return Slices.utf8Slice(Joiner.on(" ").join(output));
    }

    private static double truncateDoubleIfNeeded(double d, double roundScale)
    {
        if (roundScale <= 0.0) {
            return d;
        }
        double tmp = d * roundScale;
        double tmpSub = tmp - (long) tmp;

        long tmpRounded = (long) (d >= 0 ? (tmpSub >= 0.5f ? tmp + 1 : tmp)
                : (tmpSub >= -0.5f ? tmp : tmp - 1));

        return tmpRounded / roundScale;
    }
}
