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

import io.trino.operator.scalar.AbstractTestFunctions;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.VarcharType;
import io.trino.sql.query.QueryAssertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.ArrayList;

public class TestExtractHtmlTextFunction
        extends AbstractTestFunctions
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
    public void testBasicHtml()
    {
        assertions.function("extract_html_text", "null")
                .assertThat()
                .isNull(VarcharType.VARCHAR);

        assertions.function("extract_html_text", "''")
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo("");

        assertions.function("extract_html_text", "'<html><body>Hi<p>Hello</p></body></html>'")
                .assertThat()
                .hasType(VarcharType.VARCHAR)
                .isEqualTo("Hi Hello");
    }

    @org.testng.annotations.Test
    public void testCssFind()
    {
        assertions.function("css_find", "null", "null")
                .assertThat()
                .isNull(new ArrayType(VarcharType.VARCHAR));

        assertions.function("css_find", "''", "'abcd'")
                .assertThat()
                .hasType(new ArrayType(VarcharType.VARCHAR))
                .isEqualTo(new ArrayList<String>());

        ArrayList<String> expected = new ArrayList<>();
        expected.add("<p class=\"a\">Hi</p>");
        expected.add("<p class=\"a\">Hello</p>");
        assertions.function("css_find", "'<html><body><h1> not used</h1><p class=\"a\">Hi</p><p class=\"a\">Hello</p></body></html>'", "'p.a'")
                .assertThat()
                .hasType(new ArrayType(VarcharType.VARCHAR))
                .isEqualTo(expected);

        ArrayList<String> expected2 = new ArrayList<>();
        expected2.add("<img src=\"https://static0.xdaimages.com/press.jpg\" alt=\"Samsung\" width=\"2100\" height=\"1400\" fileSize=\"187\">");
        assertions.function("css_find", "'<img src=\"https://static0.xdaimages.com/press.jpg\" alt=\"Samsung\" width=\"2100\" height=\"1400\" fileSize=\"187\" />'", "'img'")
                .assertThat()
                .hasType(new ArrayType(VarcharType.VARCHAR))
                .isEqualTo(expected2);
    }
}
