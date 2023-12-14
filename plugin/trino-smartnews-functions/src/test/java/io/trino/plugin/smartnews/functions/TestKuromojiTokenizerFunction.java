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

//import com.google.common.collect.ImmutableList;
//import io.trino.operator.scalar.AbstractTestFunctions;
//import io.trino.spi.type.ArrayType;
//import org.testng.annotations.BeforeClass;
//
//import static io.trino.spi.type.VarcharType.createVarcharType;
//
//public class TestKuromojiTokenizerFunction
//        extends AbstractTestFunctions
//{
//    @BeforeClass
//    public void setUp()
//    {
//        functionAssertions.installPlugin(new SmartnewsFunctionsPlugin());
//    }
//
//    @org.testng.annotations.Test
//    public void testWithoutFilter()
//    {
//        assertFunction("kuro_tokenize('大島優子、紅白終了後の新年の瞬間、お尻丸出しに！後輩・指原が暴露 : スポーツ報知，勇気を出してチビッ子女上司を飲みに誘って泊めた結果ｗｗｗｗ')",
//                new ArrayType(createVarcharType(25)),
//                ImmutableList.of("大島", "大島優子", "優子", "紅白", "終了", "新年", "瞬間", "尻", "丸出し", "後輩", "指原", "暴露", "スポーツ報知", "勇気", "出す", "チビッ", "子女", "上司", "飲む", "誘う", "泊める"));
//    }
//
//    @org.testng.annotations.Test
//    public void testWithFilter()
//    {
//        assertFunction("kuro_tokenize('大島優子、紅白終了後の新年の瞬間、お尻丸出しに！後輩・指原が暴露 : スポーツ報知，勇気を出してチビッ子女上司を飲みに誘って泊めた結果ｗｗｗｗ', '名詞')",
//                new ArrayType(createVarcharType(25)),
//                ImmutableList.of("大島", "大島優子", "優子", "紅白", "終了", "新年", "瞬間", "尻", "丸出し", "後輩", "指原", "暴露", "スポーツ報知", "勇気", "チビッ", "子女", "上司"));
//    }
//}
