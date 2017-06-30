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
package com.facebook.presto.smartnews.functions;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;

import java.sql.SQLException;

/**
 * Created by lan on 1/19/16.
 */
public final class AssertFunctions
{
    private AssertFunctions()
    {
    }

    @Description("Assert expression is true, otherwise throw exception")
    @ScalarFunction("assert_true")
    @SqlType(StandardTypes.BIGINT)
    public static long assertTrue(@SqlNullable @SqlType(StandardTypes.BOOLEAN) Boolean exp) throws SQLException
    {
        if (exp == null || !exp) {
            throw new SQLException("expression isn't true");
        }
        return 1;
    }
}
