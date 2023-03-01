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

import com.smartnews.account.authentication.uuid.Base62UUIDGenerator;
import com.smartnews.account.authentication.uuid.FasterXMLUUIDGenerator;
import com.smartnews.account.id.AccountIdConverter;
import com.smartnews.pseudonymisation.service.PseudonymisationService;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.sql.SQLException;

public final class PseudonymisationFunctions
{
    private PseudonymisationFunctions()
    {
    }

    private static PseudonymisationService pseudonymisationService;
    private static AccountIdConverter accountIdConverter;

    static {
        pseudonymisationService = new PseudonymisationService();
        accountIdConverter = new AccountIdConverter(new Base62UUIDGenerator(new FasterXMLUUIDGenerator()), 10);
    }

    @Description("Convert device_token into virtual_id")
    @ScalarFunction("to_virtual_id")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice to_virtual_id(@SqlType(StandardTypes.VARCHAR) Slice slice) throws SQLException
    {
        String argument = slice.toStringUtf8();
        return Slices.utf8Slice(pseudonymisationService.pseudonymise(argument.toString()));
    }

    @Description("Convert virtual_id into account_id")
    @ScalarFunction("to_account_id")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice to_account_id(@SqlType(StandardTypes.VARCHAR) Slice slice) throws SQLException
    {
        String argument = slice.toStringUtf8();
        return Slices.utf8Slice(accountIdConverter.getBackfilledAccountIdFromVirtualId(argument.toString()));
    }
}
