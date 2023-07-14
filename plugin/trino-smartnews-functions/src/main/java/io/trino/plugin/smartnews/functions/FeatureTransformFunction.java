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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartnews.feature.transforming.FeatureTransformer;
import com.smartnews.feature.transforming.FeatureTransformerImpl;
import com.smartnews.feature.transforming.config.FeatureTransformerConfig;
import com.smartnews.feature.transforming.config.FeatureTransformerConfigFactory;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.io.FileInputStream;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

public class FeatureTransformFunction
{
    private static final TypeReference<Map<String, Object>> TYPE_REFERENCE = new TypeReference<Map<String, Object>>() {};
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static final int MAX_ENTRIES = 100;
    static final Map<Slice, FeatureTransformer> cache = new LinkedHashMap(MAX_ENTRIES + 1, .75F, true)
    {
        // This method is called just after a new entry has been added
        public boolean removeEldestEntry(Map.Entry eldest)
        {
            return size() > MAX_ENTRIES;
        }
    };

    private FeatureTransformFunction()
    {
    }

    @Description("runs feature transformation with the specified config file, which should be in S3 URI format")
    @ScalarFunction(value = "feature_transform", deterministic = false)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice transform(@SqlType(StandardTypes.VARCHAR) Slice userFeature,
                                  @SqlType(StandardTypes.VARCHAR) Slice itemFeature,
                                  @SqlType(StandardTypes.VARCHAR) Slice contextFeature,
                                  @SqlType(StandardTypes.VARCHAR) Slice configUri) throws SQLException
    {
        FeatureTransformer transformer = cache.get(configUri);
        if (transformer == null) {
            try {
                String json = null;
                String configUriStr = configUri.toStringUtf8();
                if (configUriStr.toLowerCase(Locale.ROOT).startsWith("s3://")
                        || configUriStr.toLowerCase(Locale.ROOT).startsWith("s3a://")) {
                    // S3 URI (e.g. s3://bucket-name/path/to/config.json)
                    AmazonS3URI s3URI = new AmazonS3URI(configUriStr);
                    AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
                    S3Object s3Object = s3Client.getObject(s3URI.getBucket(), s3URI.getKey());
                    json = IOUtils.toString(s3Object.getObjectContent());
                }
                else if (configUriStr.startsWith("{")) {
                    // JSON string
                    json = configUriStr;
                }
                else {
                    // local file path
                    json = IOUtils.toString(new FileInputStream(configUriStr));
                }

                FeatureTransformerConfig config = FeatureTransformerConfigFactory.from(json);
                transformer = new FeatureTransformerImpl(config);
                cache.put(configUri, transformer);
            }
            catch (Exception e) {
                throw new SQLException(e);
            }
        }
        String userFeatureJson = userFeature.toStringUtf8();
        String itemFeatureJson = itemFeature.toStringUtf8();
        String contextFeatureJson = contextFeature.toStringUtf8();
        try {
            Map<String, Object> userFeatureMap = objectMapper.readValue(userFeatureJson, TYPE_REFERENCE);
            Map<String, Object> itemFeatureMap = objectMapper.readValue(itemFeatureJson, TYPE_REFERENCE);
            Map<String, Object> contextFeatureMap = objectMapper.readValue(contextFeatureJson, TYPE_REFERENCE);
            Map<String, Object> result = transformer.transform(userFeatureMap, itemFeatureMap, contextFeatureMap);
            return Slices.utf8Slice(objectMapper.writeValueAsString(result));
        }
        catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
