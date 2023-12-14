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
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.sql.SQLException;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class ExtractHtmlTextFunction
{
    private ExtractHtmlTextFunction()
    {
    }

    @Description("Extract text from HTML")
    @ScalarFunction("extract_html_text")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice extractHtmlText(@SqlType(StandardTypes.VARCHAR) Slice slice) throws SQLException
    {
        String argument = slice.toStringUtf8();
        return Slices.utf8Slice(Jsoup.parse(argument).text());
    }

    @Description("Extract text from HTML by selector")
    @ScalarFunction("css_find")
    @SqlType("array(varchar)")
    public static Block extractHtmlText(@SqlType(StandardTypes.VARCHAR) Slice slice, @SqlType(StandardTypes.VARCHAR) Slice selector) throws SQLException
    {
        String html = slice.toStringUtf8();
        String cssSelector = selector.toStringUtf8();

        Elements elements = Jsoup.parse(html).select(cssSelector);

        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, elements != null ? elements.size() : 1);
        if (elements != null) {
            for (Element element : elements) {
                VARCHAR.writeSlice(blockBuilder, utf8Slice(element.outerHtml()));
            }
        }
        return blockBuilder.build();
    }
}
