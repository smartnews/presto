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

import java.sql.SQLException;

public class StringFunctions
{
    private StringFunctions()
    {
    }

    @Description("Compare to version string separated by dot, returns -1, 0, 1 respectively")
    @ScalarFunction("compare_version")
    @SqlType(StandardTypes.INTEGER)
    @SqlNullable
    public static Long compareVersion(@SqlType(StandardTypes.VARCHAR) Slice ver1, @SqlType(StandardTypes.VARCHAR) Slice ver2) throws SQLException
    {
        String v1 = ver1.toStringUtf8();
        String v2 = ver2.toStringUtf8();
        if (v1 != null && v2 != null) {
            String[] p1 = v1.split("\\.");
            String[] p2 = v2.split("\\.");

            int s1 = p1.length;
            int s2 = p2.length;
            int s = Math.max(s1, s2);

            try {
                for (int i = 0; i < s; i++) {
                    if (i < s1 && i < s2) {
                        if (p1[i].equals(p2[i])) {
                            continue;
                        }
                        return (long) Integer.compare(
                                Integer.parseInt(p1[i].split("-")[0]),
                                Integer.parseInt(p2[i].split("-")[0]));
                    }
                    else if (i < s1) {
                        return 1L;
                    }
                    else if (i < s2) {
                        return -1L;
                    }
                }
            }
            catch (NumberFormatException e) {
                return null;
            }

            return 0L;
        }
        return null;
    }
}
