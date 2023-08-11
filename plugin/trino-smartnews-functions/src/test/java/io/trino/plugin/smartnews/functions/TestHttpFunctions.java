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

import io.trino.sql.query.QueryAssertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public class TestHttpFunctions
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

    // The test endpoint is not stable, disable for now
    // @org.testng.annotations.Test
    public void testHttpFunctions()
    {
        assertions.query("select json_extract_scalar(http_get('https://postman-echo.com/get?foo1=bar1&foo2=bar2'), '$.args.foo1')")
                .assertThat()
                .containsAll("select varchar 'bar1'");

        assertions.query("select json_extract_scalar(try_http_get('https://postman-echo.com/get?foo1=bar1&foo2=bar2'), '$.args.foo1')")
                .assertThat()
                .containsAll("select varchar 'bar1'");

        assertions.query("select json_extract_scalar(http_post('https://postman-echo.com/post', 'bar2', map(array['Content-Type'], array['application/json'])), '$.data')")
                .assertThat()
                .containsAll("select varchar 'bar2'");

        assertions.query("select json_extract_scalar(try_http_post('https://postman-echo.com/post', 'bar2', map(array['Content-Type'], array['application/json'])), '$.data')")
                .assertThat()
                .containsAll("select varchar 'bar2'");
    }
}
