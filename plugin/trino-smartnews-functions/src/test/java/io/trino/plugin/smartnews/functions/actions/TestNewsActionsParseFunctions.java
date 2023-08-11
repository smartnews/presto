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
package io.trino.plugin.smartnews.functions.actions;

import io.trino.plugin.smartnews.functions.SmartnewsFunctionsPlugin;
import io.trino.sql.query.QueryAssertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.stream.Collectors;

public class TestNewsActionsParseFunctions
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

    static final String reportImpressionJson = "{\n" +
            "    \"action\": \"reportImpressions\",\n" +
            "    \"accountId\": \"4CfyL34AjzY55tcV79lSWa\",\n" +
            "    \"userId\": 373076615,\n" +
            "    \"userCreationTimestamp\": 1640353197,\n" +
            "    \"deviceToken\": null,\n" +
            "    \"virtualId\": \"FIOFIzWQItdedD5b6mJf4A\",\n" +
            "    \"sentTimestamp\": 1689555600,\n" +
            "    \"eventTimestamp\": 1689555587,\n" +
            "    \"eventTimestampMillisec\": 1689555587132,\n" +
            "    \"edition\": \"ja_JP\",\n" +
            "    \"originalEdition\": null,\n" +
            "    \"platform\": \"android\",\n" +
            "    \"locale\": \"ja_jp\",\n" +
            "    \"timezone\": \"Asia/Tokyo\",\n" +
            "    \"language\": \"ja\",\n" +
            "    \"country\": \"jp\",\n" +
            "    \"connectionType\": \"wifi\",\n" +
            "    \"sessionId\": \"qQhH_0P3hzn7kDeuSmpM2Q\",\n" +
            "    \"initialCountry\": \"JP\",\n" +
            "    \"initialRegion\": \"Okinawa\",\n" +
            "    \"systemVersion\": \"10\",\n" +
            "    \"appVersion\": \"23.7.10\",\n" +
            "    \"hardware\": \"samsung SCV43\",\n" +
            "    \"data\":\n" +
            "    {\n" +
            "        \"channel\": \"cr_ja_top\",\n" +
            "        \"triggerLinkId\": null,\n" +
            "        \"blockIds\":\n" +
            "        [\n" +
            "            \"b_ja_top_carousel_v1\",\n" +
            "            \"cr_ja_top\",\n" +
            "            \"b_ja_home_top_second_block\"\n" +
            "        ],\n" +
            "        \"isDarkAppearance\": true,\n" +
            "        \"trigger\":\n" +
            "        [\n" +
            "            null,\n" +
            "            null,\n" +
            "            null,\n" +
            "            null,\n" +
            "            null,\n" +
            "            null\n" +
            "        ],\n" +
            "        \"type\": \"1,1,1,1,1,1\",\n" +
            "        \"linkBlockIndices\":\n" +
            "        [\n" +
            "            2,\n" +
            "            2,\n" +
            "            2,\n" +
            "            2,\n" +
            "            2,\n" +
            "            2\n" +
            "        ],\n" +
            "        \"linkIds\":\n" +
            "        [\n" +
            "            \"4571387392191110783\"\n" +
            "        ],\n" +
            "        \"trackingTokens\":\n" +
            "        [\n" +
            "            \"1.1:01H5GNNE0E6EVG0V7199TT15KC,1.2:2,1.3:1,1.4:12,1.5:1,1.6:15,3.1:70,3.4:1,3.5:1,9.1:001977571a0a0068b26b24413e,9.2:0\"\n" +
            "        ],\n" +
            "        \"blockInventoryIndices\":\n" +
            "        [\n" +
            "            2,\n" +
            "            2,\n" +
            "            2,\n" +
            "            2,\n" +
            "            2,\n" +
            "            2\n" +
            "        ],\n" +
            "        \"viewUrl\": \"/channel/cr_ja_top\",\n" +
            "        \"blockInventories\":\n" +
            "        [\n" +
            "            \"top_card_jp\",\n" +
            "            \"top_story_jp_v2\",\n" +
            "            \"feed_jp\"\n" +
            "        ],\n" +
            "        \"trackingIds\":\n" +
            "        [\n" +
            "            \"306682216319814660843667507\",\n" +
            "            \"306682216373857856372113677\",\n" +
            "            \"306682216355843457862631490\",\n" +
            "            \"306682216373857856372113533\",\n" +
            "            \"306682216463929848919523494\",\n" +
            "            \"306682216481944247429005426\"\n" +
            "        ],\n" +
            "        \"queryLocale\": null,\n" +
            "        \"forwardedFor\": \"133.204.81.65, 10.6.177.231, 10.0.55.69\",\n" +
            "        \"countryIsoCode\": \"JP\",\n" +
            "        \"city\": \"Ginowan\",\n" +
            "        \"latitude\": 26.2636,\n" +
            "        \"postalCode\": \"901-2211\",\n" +
            "        \"ipAddress\": \"127.0.0.1\",\n" +
            "        \"region\": \"Okinawa\",\n" +
            "        \"longitude\": 127.7548\n" +
            "    },\n" +
            "    \"abtestV3VariantIds\":\n" +
            "    [\n" +
            "        4771,\n" +
            "        5109,\n" +
            "        5162,\n" +
            "        5521,\n" +
            "        6028,\n" +
            "        6706,\n" +
            "        6716\n" +
            "    ],\n" +
            "    \"signinStatus\":\n" +
            "    {\n" +
            "        \"idp\":\n" +
            "        [\n" +
            "            \"guest\"\n" +
            "        ]\n" +
            "    },\n" +
            "    \"abtest\":\n" +
            "    {\n" +
            "        \"2\":\n" +
            "        {\n" +
            "            \"1.0\": 33\n" +
            "        }\n" +
            "    }\n" +
            "}";

    @org.testng.annotations.Test
    public void testReportImpressions()
    {
        assertions.query(String.format("SELECT m.device_token, m.virtual_id, m.edition, m.platform, m.session_id, m.app_version, m.system_version" +
                        ", m.connection_type, m.locale, m.timezone, m.hardware, m.event_timestamp, m.account_id, m.user_creation_timestamp" +
                        ", m.ab_test, m.ab_test_v3_variant_ids, m.channel, m.links FROM (select parse_report_impressions('%s')) t(m)", reportImpressionJson))
                .assertThat()
                .containsAll("select " +
                        "cast(null as varchar) as device_token" +
                        ", cast('FIOFIzWQItdedD5b6mJf4A' as varchar) as virtual_id" +
                        ", cast('ja_JP' as varchar) as edition" +
                        ", cast('android' as varchar) as platform" +
                        ", cast('qQhH_0P3hzn7kDeuSmpM2Q' as varchar) as session_id" +
                        ", cast('23.7.10' as varchar) as app_version" +
                        ", cast('10' as varchar) as system_version" +
                        ", cast('wifi' as varchar) as connection_type" +
                        ", cast('ja_jp' as varchar) as locale" +
                        ", cast('Asia/Tokyo' as varchar) as timezone" +
                        ", cast('samsung SCV43' as varchar) as hardware" +
                        ", cast(1689555587 as bigint) as event_timestamp" +
                        ", cast('4CfyL34AjzY55tcV79lSWa' as varchar) as account_id" +
                        ", cast(1640353197 as bigint) as user_creation_timestamp" +
                        ", array[map(array[cast('1.0' as varchar), cast ('definitionId' as varchar)], array[33, 2])] as ab_test" +
                        ", array[4771, 5109, 5162, 5521, 6028, 6706, 6716] as ab_test_v3_variant_ids" +
                        ", cast('cr_ja_top' as varchar) as channel " +
                        ", array[cast(row(bigint '4571387392191110783', integer '0', 'b_ja_home_top_second_block', '01H5GNNE0E6EVG0V7199TT15KC', null, null) as row(link_id bigint, index integer, block varchar, req_id varchar, tracking_token map(varchar, varchar), cell_type integer))] as links");

        // null abtest
        assertions.query(String.format("SELECT cardinality(m.ab_test) FROM (select parse_report_impressions('%s')) t(m)", reportImpressionJson.replace("abtest", "abtest2")))
                .assertThat()
                .containsAll("select bigint '0'");

        // null ab_test_v3_variant_ids
        assertions.query(String.format("SELECT m.ab_test_v3_variant_ids FROM (select parse_report_impressions('%s')) t(m)", reportImpressionJson.replace("abtestV3VariantIds", "abtestV3VariantIds2")))
                .assertThat()
                .containsAll("select cast(null as array(integer)) as ab_test_v3_variant_ids");
    }

    static final String viewChannelJson = "{\n" +
            "    \"action\": \"viewChannel\",\n" +
            "    \"accountId\": \"4PnrSxcejsz9PAuWphc29f\",\n" +
            "    \"userId\": 161973575,\n" +
            "    \"userCreationTimestamp\": 1593034086,\n" +
            "    \"deviceToken\": null,\n" +
            "    \"virtualId\": \"WXLhMlDP6ZR68dS0gnKoLw\",\n" +
            "    \"sentTimestamp\": 1689555600,\n" +
            "    \"eventTimestamp\": 1689555592,\n" +
            "    \"eventTimestampMillisec\": 1689555592658,\n" +
            "    \"edition\": \"ja_JP\",\n" +
            "    \"originalEdition\": null,\n" +
            "    \"platform\": \"android\",\n" +
            "    \"locale\": \"ja_jp\",\n" +
            "    \"timezone\": \"Asia/Tokyo\",\n" +
            "    \"language\": \"ja\",\n" +
            "    \"country\": \"jp\",\n" +
            "    \"connectionType\": \"4g\",\n" +
            "    \"sessionId\": \"lw4kRtlkqaC4lZIkAlBBuA\",\n" +
            "    \"initialCountry\": \"JP\",\n" +
            "    \"initialRegion\": \"Hiroshima\",\n" +
            "    \"systemVersion\": \"11\",\n" +
            "    \"appVersion\": \"8.63.0\",\n" +
            "    \"hardware\": \"Sony SO-01M\",\n" +
            "    \"data\":\n" +
            "    {\n" +
            "        \"totalDuration\": 0.837,\n" +
            "        \"indexDuration\": 0.837,\n" +
            "        \"linkIds\":\n" +
            "        [\n" +
            "            \"4571528451927843406\"\n" +
            "        ],\n" +
            "        \"trackingTokens\":\n" +
            "        [\n" +
            "            \"1.1:01H5GNN9QC34HA0J82DESF0FHW,1.3:1,1.4:1,1.5:1,1.6:1,1.2:1,3.1:55,3.4:1,3.5:3\"\n" +
            "        ],\n" +
            "        \"channel\": \"cr_ja_top\",\n" +
            "        \"blockIds\":\n" +
            "        [\n" +
            "            \"cr_ja_top\",\n" +
            "            \"b_ja_home_top_second_block\"\n" +
            "        ],\n" +
            "        \"linkBlockIndices\":\n" +
            "        [\n" +
            "            0,\n" +
            "            0,\n" +
            "            0\n" +
            "        ],\n" +
            "        \"queryLocale\": null,\n" +
            "        \"forwardedFor\": \"49.98.79.80, 10.2.173.86, 10.0.51.60\",\n" +
            "        \"countryIsoCode\": \"JP\",\n" +
            "        \"city\": null,\n" +
            "        \"latitude\": 35.6897,\n" +
            "        \"postalCode\": null,\n" +
            "        \"ipAddress\": \"127.0.0.1\",\n" +
            "        \"region\": null,\n" +
            "        \"longitude\": 139.6895\n" +
            "    },\n" +
            "    \"abtestV3VariantIds\":\n" +
            "    [\n" +
            "        5243,\n" +
            "        6715,\n" +
            "        6857\n" +
            "    ],\n" +
            "    \"signinStatus\":\n" +
            "    {}\n" +
            "}";

    @org.testng.annotations.Test
    public void testViewChannel()
    {
        assertions.query(String.format("SELECT m.channel, m.index_duration, m.total_duration, m.links FROM (select parse_view_channel('%s')) t(m)", viewChannelJson))
                .assertThat()
                .containsAll("select " +
                        "cast('cr_ja_top' as varchar) as channel " +
                        ", double '0.837' as index_duration" +
                        ", double '0.837' as total_duration" +
                        ", array[cast(row(bigint '4571528451927843406', integer '0', 'cr_ja_top', '01H5GNN9QC34HA0J82DESF0FHW', null, null) as row(link_id bigint, index integer, block varchar, req_id varchar, tracking_token map(varchar, varchar), cell_type integer))] as links");
    }

    static final String viewOriginalPageJson = "{\n" +
            "    \"action\": \"viewOriginalPage\",\n" +
            "    \"accountId\": \"5B9sHmAkOFaF8cU3dl8Nej\",\n" +
            "    \"userId\": 411087596,\n" +
            "    \"userCreationTimestamp\": 1651962240,\n" +
            "    \"deviceToken\": null,\n" +
            "    \"virtualId\": \"P0VYTEKqfTgymxDQs4GM8A\",\n" +
            "    \"sentTimestamp\": 1689555600,\n" +
            "    \"eventTimestamp\": 1689555548,\n" +
            "    \"eventTimestampMillisec\": 1689555548239,\n" +
            "    \"edition\": \"ja_JP\",\n" +
            "    \"originalEdition\": null,\n" +
            "    \"platform\": \"android\",\n" +
            "    \"locale\": \"ja_jp\",\n" +
            "    \"timezone\": \"Asia/Tokyo\",\n" +
            "    \"language\": \"ja\",\n" +
            "    \"country\": \"jp\",\n" +
            "    \"connectionType\": \"wifi\",\n" +
            "    \"sessionId\": \"mwA4OPXy7xpFFBlYYkBJ7A\",\n" +
            "    \"initialCountry\": \"JP\",\n" +
            "    \"initialRegion\": \"Tokyo\",\n" +
            "    \"systemVersion\": \"13\",\n" +
            "    \"appVersion\": \"22.4.40\",\n" +
            "    \"hardware\": \"SHARP SHG03\",\n" +
            "    \"data\":\n" +
            "    {\n" +
            "        \"articleViewStyle\": \"SMART\",\n" +
            "        \"block\": \"b_ja_home_top_second_block_part_YDrtNJgX\",\n" +
            "        \"channel\": \"cr_ja_top\",\n" +
            "        \"commentCount\": null,\n" +
            "        \"commentIds\":\n" +
            "        [],\n" +
            "        \"linkId\": \"4570952920878551997\",\n" +
            "        \"loadTime\": -1.0,\n" +
            "        \"loadTimeSmart\": 1.376,\n" +
            "        \"originalPageDuration\": 0.002,\n" +
            "        \"placement\": \"default\",\n" +
            "        \"reactionCount\": null,\n" +
            "        \"readTimeSmart\": 15.736,\n" +
            "        \"readTimeWeb\": 0.0,\n" +
            "        \"readerDuration\": 15.736,\n" +
            "        \"trackingToken\": \"1.1:01H5GN02QKEYH51SPVWSV90YDB,1.2:2,1.3:1,1.4:30,1.5:1,1.6:53,3.1:70,3.4:1,3.5:1,9.1:001977545e0a006a8d7db9b6d4,9.2:0\",\n" +
            "        \"url\": \"https://dog.benesse.ne.jp/lovedog/content/?id=162558\",\n" +
            "        \"viewRatioSmart\": 0.701410174369812,\n" +
            "        \"viewRatioWeb\": 0.0,\n" +
            "        \"queryLocale\": null,\n" +
            "        \"forwardedFor\": \"27.82.153.206, 10.6.190.105, 10.0.51.60\",\n" +
            "        \"countryIsoCode\": \"JP\",\n" +
            "        \"city\": \"Miyamotocho\",\n" +
            "        \"latitude\": 35.8846,\n" +
            "        \"postalCode\": \"343-0801\",\n" +
            "        \"ipAddress\": \"127.0.0.1\",\n" +
            "        \"region\": \"Saitama\",\n" +
            "        \"longitude\": 139.7753\n" +
            "    },\n" +
            "    \"abtestV3VariantIds\":\n" +
            "    [\n" +
            "        6717,\n" +
            "        6623,\n" +
            "        6744\n" +
            "    ],\n" +
            "    \"signinStatus\":\n" +
            "    {\n" +
            "        \"idp\":\n" +
            "        [\n" +
            "            \"guest\"\n" +
            "        ]\n" +
            "    }\n" +
            "}";

    @org.testng.annotations.Test
    public void testViewOriginalPage()
    {
        assertions.query(String.format("SELECT m.link_id, m.url, m.channel, m.placement, m.read_time_web, m.read_time_smart, m.view_ratio_web, m.view_ratio_smart, m.block, m.req_id, m.tracking_token FROM (select parse_view_original_page('%s')) t(m)", viewOriginalPageJson))
                .assertThat()
                .containsAll("select " +
                        "bigint '4570952920878551997' as link_id " +
                        ", varchar 'https://dog.benesse.ne.jp/lovedog/content/?id=162558' as url " +
                        ", varchar 'cr_ja_top' as channel " +
                        ", varchar 'default' as placement " +
                        ", double '0.0' as read_time_web " +
                        ", double '15.736' as read_time_smart " +
                        ", double '0.0' as view_ratio_web " +
                        ", double '0.701410174369812' as view_ratio_smart " +
                        ", varchar 'b_ja_home_top_second_block_part_YDrtNJgX' as block " +
                        ", varchar '01H5GN02QKEYH51SPVWSV90YDB' as req_id " +
                        ", map(ARRAY[varchar '1.1', varchar '1.2',varchar  '1.3',varchar  '1.4',varchar  '1.5',varchar  '1.6',varchar  '3.1',varchar  '3.4',varchar  '3.5', varchar '9.1', varchar '9.2'], ARRAY[varchar '01H5GN02QKEYH51SPVWSV90YDB', varchar '2', varchar '1', varchar '30', varchar '1', varchar '53', varchar '70', varchar '1', varchar '1', varchar '001977545e0a006a8d7db9b6d4', varchar '0']) as tracking_token");
    }

    static final String viewOpenArticleJson = "{\n" +
            "    \"action\": \"openArticle\",\n" +
            "    \"accountId\": \"58nImDyy7pINRwYCEyvgzX\",\n" +
            "    \"userId\": 15137852,\n" +
            "    \"userCreationTimestamp\": 1432072544,\n" +
            "    \"deviceToken\": null,\n" +
            "    \"virtualId\": \"v6U9+JyNp69R29ZVQc0LKQ\",\n" +
            "    \"sentTimestamp\": 1689557117,\n" +
            "    \"eventTimestamp\": 1689557117,\n" +
            "    \"eventTimestampMillisec\": 1689557117479,\n" +
            "    \"edition\": \"ja_JP\",\n" +
            "    \"originalEdition\": null,\n" +
            "    \"platform\": \"ios\",\n" +
            "    \"locale\": \"ja_us\",\n" +
            "    \"timezone\": \"America/Los_Angeles\",\n" +
            "    \"language\": \"ja\",\n" +
            "    \"country\": \"us\",\n" +
            "    \"connectionType\": \"wifi\",\n" +
            "    \"sessionId\": \"N-pIFiSdSreN2m2D6VDJ6w\",\n" +
            "    \"initialCountry\": \"US\",\n" +
            "    \"initialRegion\": \"California\",\n" +
            "    \"systemVersion\": \"16.5.1\",\n" +
            "    \"appVersion\": \"23.7.20\",\n" +
            "    \"hardware\": \"iPhone12,5\",\n" +
            "    \"data\":\n" +
            "    {\n" +
            "        \"isUnifiedFeed\": true,\n" +
            "        \"channel\": \"cr_ja_top\",\n" +
            "        \"trackingToken\": \"1.1:01H5GQ6TF5PP4N1BK907XF1NS8,1.3:1,1.4:2,1.5:1,1.6:2,1.2:5,3.1:55,3.4:1,3.5:6\",\n" +
            "        \"placement\": \"default\",\n" +
            "        \"type\": \"1\",\n" +
            "        \"blockIdentifier\": \"cr_ja_top\",\n" +
            "        \"url\": \"https://www.sponichi.co.jp/sports/news/2023/07/17/kiji/20230717s00028000157000c.html\",\n" +
            "        \"articleViewStyle\": \"SMART\",\n" +
            "        \"block\": \"cr_ja_top\",\n" +
            "        \"linkId\": \"4571482733582231224\",\n" +
            "        \"blockInventory\": \"top_story_jp_v2\",\n" +
            "        \"blockGroupId\": \"cr_ja_top\",\n" +
            "        \"isDarkAppearance\": false,\n" +
            "        \"queryLocale\": null,\n" +
            "        \"forwardedFor\": \"76.94.70.145, 18.237.183.111, 10.2.181.152, 10.0.52.137\",\n" +
            "        \"countryIsoCode\": \"US\",\n" +
            "        \"city\": \"Los Angeles\",\n" +
            "        \"latitude\": 34.0757,\n" +
            "        \"postalCode\": \"90004\",\n" +
            "        \"ipAddress\": \"127.0.0.1\",\n" +
            "        \"region\": \"California\",\n" +
            "        \"longitude\": -118.3161\n" +
            "    },\n" +
            "    \"abtestV3VariantIds\":\n" +
            "    [\n" +
            "        4881,\n" +
            "        5108,\n" +
            "        5637,\n" +
            "        6028,\n" +
            "        6717,\n" +
            "        6802,\n" +
            "        6829\n" +
            "    ],\n" +
            "    \"signinStatus\":\n" +
            "    {\n" +
            "        \"idp\":\n" +
            "        [\n" +
            "            \"guest\"\n" +
            "        ]\n" +
            "    }\n" +
            "}";

    @org.testng.annotations.Test
    public void testViewOpenArticle()
    {
        assertions.query(String.format("SELECT m.link_id, m.url, m.channel, m.placement, m.article_view_style, m.block, m.req_id, m.tracking_token, m.cell_type FROM (select parse_open_article('%s')) t(m)", viewOpenArticleJson))
                .assertThat()
                .containsAll("select " +
                        "bigint '4571482733582231224' as link_id " +
                        ", varchar 'https://www.sponichi.co.jp/sports/news/2023/07/17/kiji/20230717s00028000157000c.html' as url " +
                        ", varchar 'cr_ja_top' as channel " +
                        ", varchar 'default' as placement " +
                        ", varchar 'SMART' as article_view_style " +
                        ", varchar 'cr_ja_top' as block " +
                        ", varchar '01H5GQ6TF5PP4N1BK907XF1NS8' as req_id " +
                        ", map(ARRAY[varchar '1.1', varchar '1.2', varchar '1.3',varchar '1.4',varchar  '1.5',varchar  '1.6',varchar  '3.1',varchar  '3.4',varchar '3.5'], " +
                        "ARRAY[varchar '01H5GQ6TF5PP4N1BK907XF1NS8', varchar '5', varchar '1', varchar '2', varchar '1', varchar '2', varchar '55', varchar '1', varchar '6']) as tracking_token " +
                        ", integer '1' as cell_type");
    }

    static final String postJson = "{\n" +
            "    \"action\": \"post\",\n" +
            "    \"accountId\": \"5qGEyrxATYTFYmhhA81yeO\",\n" +
            "    \"userId\": 257079829,\n" +
            "    \"userCreationTimestamp\": 1621512379,\n" +
            "    \"deviceToken\": null,\n" +
            "    \"virtualId\": \"DVmHvscMee2vfrs22vQ5mQ\",\n" +
            "    \"sentTimestamp\": 1689557071,\n" +
            "    \"eventTimestamp\": 1689557065,\n" +
            "    \"eventTimestampMillisec\": 1689557065441,\n" +
            "    \"edition\": \"ja_JP\",\n" +
            "    \"originalEdition\": null,\n" +
            "    \"platform\": \"ios\",\n" +
            "    \"locale\": \"ja_jp\",\n" +
            "    \"timezone\": \"Asia/Tokyo\",\n" +
            "    \"language\": \"ja\",\n" +
            "    \"country\": \"jp\",\n" +
            "    \"connectionType\": \"wifi\",\n" +
            "    \"sessionId\": \"s5c_c7hnQFytKx82QbXYUA\",\n" +
            "    \"initialCountry\": \"JP\",\n" +
            "    \"initialRegion\": \"Tokyo\",\n" +
            "    \"systemVersion\": \"16.5.1\",\n" +
            "    \"appVersion\": \"23.7.20\",\n" +
            "    \"hardware\": \"iPhone13,3\",\n" +
            "    \"data\":\n" +
            "    {\n" +
            "        \"status\": \"completed\",\n" +
            "        \"linkId\": \"4571518095268519446\",\n" +
            "        \"trackingToken\": \"1.1:01H5GPXHB8GXF00FPYDZAS1CEK,1.2:2,1.3:1,1.4:3,1.5:1,1.6:6,3.1:70,3.4:1,3.5:1,9.1:0019775c3c0a00682e120601d4,9.2:0\",\n" +
            "        \"extra\": \"{\\\"sharedURLs\\\":[]}\",\n" +
            "        \"placement\": \"article-container\",\n" +
            "        \"channel\": \"cr_ja_top\",\n" +
            "        \"trackingId\": \"306705887167398526130552898\",\n" +
            "        \"service\": \"com.apple.UIKit.activity.PostToTwitter\",\n" +
            "        \"trigger\": \"actionButton\",\n" +
            "        \"type\": \"topBarShareButton\",\n" +
            "        \"url\": \"https://www.itmedia.co.jp/fav/articles/2307/17/news030.html\",\n" +
            "        \"queryLocale\": null,\n" +
            "        \"forwardedFor\": \"106.155.13.227, 10.6.184.97, 10.0.55.252\",\n" +
            "        \"countryIsoCode\": \"JP\",\n" +
            "        \"city\": \"Osaka\",\n" +
            "        \"latitude\": 34.6986,\n" +
            "        \"postalCode\": \"530-0041\",\n" +
            "        \"ipAddress\": \"127.0.0.1\",\n" +
            "        \"region\": \"Osaka\",\n" +
            "        \"longitude\": 135.5079\n" +
            "    },\n" +
            "    \"abtestV3VariantIds\":\n" +
            "    [\n" +
            "        4233,\n" +
            "        5735,\n" +
            "        6716,\n" +
            "        6745,\n" +
            "        6827\n" +
            "    ],\n" +
            "    \"signinStatus\":\n" +
            "    {\n" +
            "        \"idp\":\n" +
            "        [\n" +
            "            \"guest\"\n" +
            "        ]\n" +
            "    }\n" +
            "}";

    @org.testng.annotations.Test
    public void testPost()
    {
        assertions.query(String.format("SELECT m.link_id, m.url, m.service, m.req_id, m.tracking_token FROM (select parse_post('%s')) t(m)", postJson))
                .assertThat()
                .containsAll("select " +
                        "bigint '4571518095268519446' as link_id " +
                        ", varchar 'https://www.itmedia.co.jp/fav/articles/2307/17/news030.html' as url " +
                        ", varchar 'com.apple.UIKit.activity.PostToTwitter' as service " +
                        ", varchar '01H5GPXHB8GXF00FPYDZAS1CEK' as req_id " +
                        ", map(ARRAY[varchar '1.1',                 varchar '1.2', varchar '1.3',varchar '1.4',varchar '1.5',varchar '1.6',varchar  '3.1',varchar  '3.4',   varchar '3.5',varchar '9.1',                        varchar '9.2'], " +
                        "ARRAY[varchar '01H5GPXHB8GXF00FPYDZAS1CEK', varchar '2', varchar '1', varchar '3',     varchar '1', varchar '6', varchar '70', varchar '1',        varchar '1', varchar '0019775c3c0a00682e120601d4', varchar '0']) as tracking_token ");

        // null tracking token
        assertions.query(String.format("SELECT m.tracking_token FROM (select parse_post('%s')) t(m)", postJson.replace("trackingToken", "trackingToken2")))
                .assertThat()
                .containsAll("select cast(null as map(varchar, varchar)) as tracking_token ");
    }

    static final String reportJson = "{\n" +
            "    \"action\": \"report\",\n" +
            "    \"accountId\": \"4wuo6k9JdMp7YZxUJx2fnU\",\n" +
            "    \"userId\": 400263383,\n" +
            "    \"userCreationTimestamp\": 1646503249,\n" +
            "    \"deviceToken\": null,\n" +
            "    \"virtualId\": \"iWNUMAVTSAKzuD/y6i13FA\",\n" +
            "    \"sentTimestamp\": 1689557114,\n" +
            "    \"eventTimestamp\": 1689556678,\n" +
            "    \"eventTimestampMillisec\": 1689556678085,\n" +
            "    \"edition\": \"ja_JP\",\n" +
            "    \"originalEdition\": null,\n" +
            "    \"platform\": \"android\",\n" +
            "    \"locale\": \"ja_jp\",\n" +
            "    \"timezone\": \"Asia/Tokyo\",\n" +
            "    \"language\": \"ja\",\n" +
            "    \"country\": \"jp\",\n" +
            "    \"connectionType\": \"4g\",\n" +
            "    \"sessionId\": \"hp-fR2MbKlZ6FpXKFfZCjA\",\n" +
            "    \"initialCountry\": \"JP\",\n" +
            "    \"systemVersion\": \"10\",\n" +
            "    \"appVersion\": \"23.7.20\",\n" +
            "    \"hardware\": \"samsung SC-54A\",\n" +
            "    \"data\":\n" +
            "    {\n" +
            "        \"trackingToken\": \"1.1:01H5GPJ2P2WXKD0DJS7MEG1JAV,1.2:2,1.3:1,1.4:12,1.5:1,1.6:15,3.1:70,3.4:1,3.5:1,9.1:0019775ac50a0069a1da729738,9.2:0\",\n" +
            "        \"linkId\": \"4571217992855063958\",\n" +
            "        \"url\": \"https://www.sponichi.co.jp/entertainment/news/2023/07/16/kiji/20230716s00041000312000c.html\",\n" +
            "        \"trackingId\": \"306699127228329046003171462\",\n" +
            "        \"queryLocale\": null,\n" +
            "        \"forwardedFor\": \"49.106.125.187, 10.2.182.211, 10.0.51.180\",\n" +
            "        \"countryIsoCode\": \"JP\",\n" +
            "        \"city\": null,\n" +
            "        \"latitude\": 35.6897,\n" +
            "        \"postalCode\": null,\n" +
            "        \"ipAddress\": \"127.0.0.1\",\n" +
            "        \"region\": null,\n" +
            "        \"longitude\": 139.6895\n" +
            "    },\n" +
            "    \"abtestV3VariantIds\":\n" +
            "    [\n" +
            "        4771,\n" +
            "        5109,\n" +
            "        5522,\n" +
            "        5608,\n" +
            "        6714,\n" +
            "        6706,\n" +
            "        6857\n" +
            "    ],\n" +
            "    \"signinStatus\":\n" +
            "    {\n" +
            "        \"idp\":\n" +
            "        [\n" +
            "            \"guest\"\n" +
            "        ]\n" +
            "    }\n" +
            "}";

    @org.testng.annotations.Test
    public void testReport()
    {
        assertions.query(String.format("SELECT m.url, m.link_id, m.req_id, m.tracking_token FROM (select parse_report('%s')) t(m)", reportJson))
                .assertThat()
                .containsAll("select " +
                        "varchar 'https://www.sponichi.co.jp/entertainment/news/2023/07/16/kiji/20230716s00041000312000c.html' as url " +
                        ", bigint '4571217992855063958' as link_id " +
                        ", varchar '01H5GPJ2P2WXKD0DJS7MEG1JAV' as req_id " +
                        ", map(ARRAY[varchar '1.1',                 varchar '1.2', varchar '1.3',varchar '1.4', varchar '1.5',varchar '1.6',varchar  '3.1',varchar  '3.4',   varchar '3.5',varchar '9.1',                        varchar '9.2'], " +
                        "ARRAY[varchar '01H5GPJ2P2WXKD0DJS7MEG1JAV', varchar '2', varchar '1',   varchar '12',  varchar '1', varchar '15', varchar '70', varchar '1',        varchar '1', varchar '0019775ac50a0069a1da729738', varchar '0']) as tracking_token ");
    }

    static final String chooseNotInterestedJson = "{\n" +
            "    \"action\": \"chooseNotInterested\",\n" +
            "    \"accountId\": \"4mSJZUyBx0WFl4TOaJPTPT\",\n" +
            "    \"userId\": 173565529,\n" +
            "    \"userCreationTimestamp\": 1596954596,\n" +
            "    \"deviceToken\": null,\n" +
            "    \"virtualId\": \"6g9Ev13T7LYyem89HngC+g\",\n" +
            "    \"sentTimestamp\": 1689557300,\n" +
            "    \"eventTimestamp\": 1689557296,\n" +
            "    \"eventTimestampMillisec\": 1689557296318,\n" +
            "    \"edition\": \"ja_JP\",\n" +
            "    \"originalEdition\": null,\n" +
            "    \"platform\": \"ios\",\n" +
            "    \"locale\": \"ja_jp\",\n" +
            "    \"timezone\": \"Asia/Tokyo\",\n" +
            "    \"language\": \"ja\",\n" +
            "    \"country\": \"jp\",\n" +
            "    \"connectionType\": \"wifi\",\n" +
            "    \"sessionId\": \"Fm7NHxr7ToW0puCKo_KWHQ\",\n" +
            "    \"initialCountry\": \"JP\",\n" +
            "    \"initialRegion\": \"Aomori\",\n" +
            "    \"systemVersion\": \"16.5.1\",\n" +
            "    \"appVersion\": \"23.7.20\",\n" +
            "    \"hardware\": \"iPhone11,2\",\n" +
            "    \"data\":\n" +
            "    {\n" +
            "        \"linkId\": \"4571208132272858413\",\n" +
            "        \"channel\": \"cr_ja_top\",\n" +
            "        \"trackingToken\": \"1.1:01H5GQACY5XH6Z1RWVXXGX112T,1.2:2,1.3:1,1.4:12,1.5:1,1.6:12,3.1:40,3.4:5,3.5:1,9.1:0019775c910a006ae61fbf10ff,9.2:1\",\n" +
            "        \"block\": \"b_ja_home_top_second_block_part_EFBYnEyE\",\n" +
            "        \"trackingId\": \"306713479101463166692753547\",\n" +
            "        \"publisherName\": \"デイリースポーツOnline\",\n" +
            "        \"url\": \"https://www.daily.co.jp/gossip/2023/07/16/0016591135.shtml\",\n" +
            "        \"queryLocale\": null,\n" +
            "        \"forwardedFor\": \"49.96.237.238, 10.2.183.13, 10.0.52.217\",\n" +
            "        \"countryIsoCode\": \"JP\",\n" +
            "        \"city\": \"Shibuya\",\n" +
            "        \"latitude\": 35.6792,\n" +
            "        \"postalCode\": \"151-0051\",\n" +
            "        \"ipAddress\": \"127.0.0.1\",\n" +
            "        \"region\": \"Tokyo\",\n" +
            "        \"linkBlockIndices\":[11,22],\n" +
            "        \"longitude\": 139.6992\n" +
            "    },\n" +
            "    \"abtestV3VariantIds\":\n" +
            "    [\n" +
            "        4234,\n" +
            "        5107,\n" +
            "        6717,\n" +
            "        6617,\n" +
            "        6829\n" +
            "    ],\n" +
            "    \"signinStatus\":\n" +
            "    {\n" +
            "        \"idp\":\n" +
            "        [\n" +
            "            \"guest\"\n" +
            "        ]\n" +
            "    }\n" +
            "}";

    @org.testng.annotations.Test
    public void testChooseNotInterested()
    {
        assertions.query(String.format("SELECT  m.link_id, m.publisher_name, m.url, m.link_block_indices, m.city, m.region, m.country_iso_code, m.lat, m.lon FROM (select parse_choose_not_interested('%s')) t(m)", chooseNotInterestedJson))
                .assertThat()
                .containsAll("select " +
                        "bigint '4571208132272858413' as link_id " +
                        ", varchar 'デイリースポーツOnline' as publisher_name " +
                        ", varchar 'https://www.daily.co.jp/gossip/2023/07/16/0016591135.shtml' as url " +
                        ", array[integer '11', integer '22'] as link_block_indices " +
                        ", varchar 'Shibuya' as city " +
                        ", varchar 'Tokyo' as region " +
                        ", varchar 'JP' as country_iso_code " +
                        ", double '35.6792' as lat " +
                        ", double '139.6992' as lon");
    }

    static final String feedSurveyThumbClickJson = "{\n" +
            "    \"action\": \"feedSurveyThumbClick\",\n" +
            "    \"accountId\": \"4WjTJqoOA8nABY1GICvBrG\",\n" +
            "    \"userId\": 346776479,\n" +
            "    \"userCreationTimestamp\": 1635455489,\n" +
            "    \"deviceToken\": null,\n" +
            "    \"virtualId\": \"eXmx3tbFjcQmkvSZJ3cklg\",\n" +
            "    \"sentTimestamp\": 1689557438,\n" +
            "    \"eventTimestamp\": 1689557419,\n" +
            "    \"eventTimestampMillisec\": 1689557419990,\n" +
            "    \"edition\": \"ja_JP\",\n" +
            "    \"originalEdition\": null,\n" +
            "    \"platform\": \"android\",\n" +
            "    \"locale\": \"ja_jp\",\n" +
            "    \"timezone\": \"Asia/Tokyo\",\n" +
            "    \"language\": \"ja\",\n" +
            "    \"country\": \"jp\",\n" +
            "    \"connectionType\": \"wifi\",\n" +
            "    \"sessionId\": \"t5fF1159yo0ftO9WqKZGpg\",\n" +
            "    \"initialCountry\": \"JP\",\n" +
            "    \"initialRegion\": \"Kochi\",\n" +
            "    \"systemVersion\": \"10\",\n" +
            "    \"appVersion\": \"23.7.20\",\n" +
            "    \"hardware\": \"SHARP SHV42\",\n" +
            "    \"data\":\n" +
            "    {\n" +
            "        \"reactionType\": \"down\",\n" +
            "        \"publisherName\": \"スポーツ報知\",\n" +
            "        \"linkId\": \"4571515708130723157\",\n" +
            "        \"channel\": \"cr_ja_top\",\n" +
            "        \"block\": \"b_ja_home_top_second_block\",\n" +
            "        \"trackingToken\": \"1.1:01H5GQF9KRCD821BXAYQ5J1P45,1.2:2,1.3:1,1.4:2,1.5:1,1.6:5,3.1:70,3.4:5,3.5:1,9.1:0019775e820a006a009779fbf3,9.2:0\",\n" +
            "        \"trackingId\": \"306716371817547022295105627\",\n" +
            "        \"queryLocale\": null,\n" +
            "        \"forwardedFor\": \"183.77.2.2, 10.2.147.120, 10.0.52.217\",\n" +
            "        \"countryIsoCode\": \"JP\",\n" +
            "        \"city\": \"Kyoto\",\n" +
            "        \"latitude\": 35.0203,\n" +
            "        \"postalCode\": \"602-8051\",\n" +
            "        \"ipAddress\": \"127.0.0.1\",\n" +
            "        \"region\": \"Kyoto\",\n" +
            "        \"longitude\": 135.7602\n" +
            "    },\n" +
            "    \"abtestV3VariantIds\":\n" +
            "    [\n" +
            "        4771,\n" +
            "        5109,\n" +
            "        5522,\n" +
            "        6125,\n" +
            "        5608,\n" +
            "        6716,\n" +
            "        6770\n" +
            "    ],\n" +
            "    \"signinStatus\":\n" +
            "    {\n" +
            "        \"idp\":\n" +
            "        [\n" +
            "            \"guest\"\n" +
            "        ]\n" +
            "    }\n" +
            "}";

    @org.testng.annotations.Test
    public void testFeedSurveyThumbClick()
    {
        assertions.query(String.format("SELECT  m.link_id, m.reaction_type, m.city, m.region, m.country_iso_code, m.lat, m.lon FROM (select parse_feed_survey_thumb_click('%s')) t(m)", feedSurveyThumbClickJson))
                .assertThat()
                .containsAll("select " +
                        "bigint '4571515708130723157' as link_id " +
                        ", varchar 'down' as reaction_type " +
                        ", varchar 'Kyoto' as city " +
                        ", varchar 'Kyoto' as region " +
                        ", varchar 'JP' as country_iso_code " +
                        ", double '35.0203' as lat " +
                        ", double '135.7602' as lon");
    }

    @org.testng.annotations.Test
    public void printType()
    {
        String rowTypeString = ChooseNotInterestedParseFunctions.actionSpecificTypes.stream()
                .map(r -> r.name + " " + r.type.getDisplayName())
                .collect(Collectors.joining(", "));
        System.out.println(rowTypeString);
    }
}
