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

import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeUtils;
import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.Request;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;

/**
 * Created by lan on 1/19/16.
 */
public final class HttpFunctions
{
    private static final HttpClient HTTP_CLIENT = new JettyHttpClient(
            new HttpClientConfig()
                    .setMaxConnections(512)
                    .setMaxConnectionsPerServer(64)
    );

    private HttpFunctions()
    {
        HttpClientConfig config = new HttpClientConfig();
    }

    private static Slice performHttp(String url, String method, Map<String, String> headers)
    {
        try {
            Request.Builder builder = new Request.Builder().setMethod(method).setUri(new URI(url));
            if (headers != null) {
                for (Map.Entry<String, String> h : headers.entrySet()) {
                    builder.setHeader(h.getKey(), h.getValue());
                }
            }

            String body = HTTP_CLIENT.execute(builder.build(), createStringResponseHandler()).getBody();
            return Slices.utf8Slice(body);
        }
        catch (URISyntaxException e) {
            return null;
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT,
                    "Error when access " + url + ":\n" + e.toString());
        }
    }

    @ScalarFunction("http_get")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice httpGet(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        return performHttp(slice.toStringUtf8(), "GET", null);
    }

    @ScalarFunction("http_get")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    @TypeParameter("V")
    public static Slice httpGet(
            @TypeParameter("V") Type valueType,
            @SqlType(StandardTypes.VARCHAR) Slice slice,
            @SqlType("map<V,V>") Block headers)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (int i = 0; i < headers.getPositionCount(); i += 2) {
            builder.put(
                    ((Slice) TypeUtils.readNativeValue(valueType, headers, i)).toStringUtf8(),
                    ((Slice) TypeUtils.readNativeValue(valueType, headers, i + 1)).toStringUtf8()
            );
        }
        return performHttp(slice.toStringUtf8(), "GET", builder.build());
    }

    @ScalarFunction("try_http_get")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice tryHttpGet(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        try {
            return performHttp(slice.toStringUtf8(), "GET", null);
        }
        catch (Exception e) {
            return null;
        }
    }

    @ScalarFunction("http_post")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice httpPost(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        return performHttp(slice.toStringUtf8(), "POST", null);
    }

    @ScalarFunction("http_post")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    @TypeParameter("V")
    public static Slice httpPost(
            @TypeParameter("V") Type valueType,
            @SqlType(StandardTypes.VARCHAR) Slice slice,
            @SqlType("map<V,V>") Block headers)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (int i = 0; i < headers.getPositionCount(); i += 2) {
            builder.put(
                    ((Slice) TypeUtils.readNativeValue(valueType, headers, i)).toStringUtf8(),
                    ((Slice) TypeUtils.readNativeValue(valueType, headers, i + 1)).toStringUtf8()
            );
        }
        return performHttp(slice.toStringUtf8(), "POST", builder.build());
    }
}
