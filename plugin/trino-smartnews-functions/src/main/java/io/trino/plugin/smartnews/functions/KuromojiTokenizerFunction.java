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
//
//import com.smartnews.nlp.analysis.ja.SmartJapaneseAnalyzer;
//import io.airlift.slice.Slice;
//import io.trino.spi.block.Block;
//import io.trino.spi.block.BlockBuilder;
//import io.trino.spi.function.Description;
//import io.trino.spi.function.ScalarFunction;
//import io.trino.spi.function.SqlType;
//import io.trino.spi.type.StandardTypes;
//
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
//import static io.trino.spi.type.VarcharType.VARCHAR;
//
//public class KuromojiTokenizerFunction
//{
//    private static Map<Slice, SmartJapaneseAnalyzer> analyzerMap = new HashMap<Slice, SmartJapaneseAnalyzer>();
//    private static SmartJapaneseAnalyzer defaultAnalyzer; // with no filter tag
//
//    private KuromojiTokenizerFunction() {}
//
//    @Description("break a Japanese string down into a stream of tokens.")
//    @ScalarFunction("kuro_tokenize")
//    @SqlType("array(varchar(x))")
//    public static Block kuro_tokenize(@SqlType(StandardTypes.VARCHAR) Slice slice) throws IOException
//    {
//        if (defaultAnalyzer == null) {
//            // defaultAnalyzer = SmartJapaneseAnalyzer.builder().whitetags(new HashSet<>()).whitetagsMatchPrefix(true).build();
//        }
//
//        return kuro_tokenize(slice, defaultAnalyzer);
//    }
//
//    @Description("break a Japanese string down into a stream of tokens.")
//    @ScalarFunction("kuro_tokenize")
//    @SqlType("array(varchar(x))")
//    public static Block kuro_tokenize(@SqlType(StandardTypes.VARCHAR) Slice slice, @SqlType(StandardTypes.VARCHAR) Slice tagFilter) throws IOException
//    {
//        SmartJapaneseAnalyzer analyzer = null;
//        if (tagFilter != null) {
//            if (analyzerMap.containsKey(tagFilter)) {
//                analyzer = analyzerMap.get(tagFilter);
//            }
//            else {
//                String tagsStr = tagFilter.toStringUtf8();
//                String[] tags = tagsStr.split(",");
//                Set<String> whiteTags = new HashSet<>();
//                for (String tag : tags) {
//                    whiteTags.add(tag);
//                }
//                // analyzer = SmartJapaneseAnalyzer.builder().whitetags(whiteTags).whitetagsMatchPrefix(true).build();
//                analyzerMap.put(tagFilter, analyzer);
//            }
//        }
//        else {
//            analyzer = defaultAnalyzer;
//        }
//        return kuro_tokenize(slice, analyzer);
//    }
//
//    public static Block kuro_tokenize(@SqlType(StandardTypes.VARCHAR) Slice slice, SmartJapaneseAnalyzer analyzer) throws IOException
//    {
//        String argument = slice.toStringUtf8();
//        List<String> tokens = analyzer.articleTokenize(argument);
//        BlockBuilder parts = VARCHAR.createBlockBuilder(null, tokens == null ? 0 : tokens.size(), tokens == null ? 0 : tokens.get(0).length());
//        if (tokens != null) {
//            for (String token : tokens) {
//                VARCHAR.writeString(parts, token);
//            }
//        }
//
//        return parts.build();
//    }
//}
